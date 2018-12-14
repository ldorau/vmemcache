#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "os_thread.h"
#include "util.h"
#include "out.h"
#include "critnib.h"

/*
 * WARNING: this implementation fails badly if you try to store two keys
 * where one is a prefix of another.  Pass a struct { int len; char[] key; }
 * or such if such keys are possible.
 */

/*
 * SLICE may be 1, 2, 4 or 8.  1 or 8 could be further optimized (critbit
 * and critbyte respectively); 4 (critnib) strikes a good balance between
 * speed and memory use.
 */
#define SLICE 4
#define NIB ((1ULL << SLICE) - 1)
#define SLNODES (1 << SLICE)

typedef uint32_t byten_t;
typedef unsigned char bitn_t;

struct critnib_node {
	struct critnib_node *child[SLNODES];
	byten_t byte;
	bitn_t bit;
};

struct critnib_leaf {
	const char *key;
	byten_t key_len;
	void *value;
};

struct critnib {
	struct critnib_node *root;
	os_mutex_t mutex;
};

/*
 * internal: is_leaf -- check tagged pointer for leafness
 */
static inline bool
is_leaf(struct critnib_node *n)
{
	return (uint64_t)n & 1;
}

/*
 * internal: to_leaf -- untag a leaf pointer
 */
static inline struct critnib_leaf *
to_leaf(struct critnib_node *n)
{
	return (void *)((uint64_t)n & ~1ULL);
}

/*
 * internal: slice_index -- get index of radix child at a given shift
 */
static inline int
slice_index(char b, bitn_t bit)
{
	return (b >> bit) & NIB;
}

/*
 * critnib_new -- allocate a new hashmap
 */
struct critnib *
critnib_new(void)
{
	struct critnib *c = malloc(sizeof(struct critnib));
	if (!c)
		return NULL;
	os_mutex_init(&c->mutex);
	c->root = NULL;
	return c;
}

/*
 * internal: recursively free a subtree
 */
static void
delete_node(struct critnib_node *n)
{
	if (!n)
		return;
	if (is_leaf(n))
		return free(to_leaf(n));
	for (int i = 0; i < SLNODES; i++)
		delete_node(n->child[i]);
}

/*
 * critnib_delete -- free a hashmap
 */
void
critnib_delete(struct critnib *c)
{
	os_mutex_destroy(&c->mutex);
	delete_node(c->root);
	free(c);
}

/*
 * internal: zalloc a node
 */
static struct critnib_node *
alloc_node(struct critnib *c)
{
	struct critnib_node *n = malloc(sizeof(struct critnib_node));
	if (!n)
		return NULL;
	for (int i = 0; i < SLNODES; i++)
		n->child[i] = NULL;
	return n;
}

/*
 * internal: alloc a leaf
 */
static struct critnib_leaf *
alloc_leaf(struct critnib *c)
{
	return malloc(sizeof(struct critnib_leaf));
}

/*
 * internal: find any leaf in a subtree
 *
 * We know they're all identical up to the divergence point between a prefix
 * shared by all of them vs the new key we're inserting.
 */
static struct critnib_node *
any_leaf(struct critnib_node *n)
{
	for (int i = 0; i < SLNODES; i++) {
		struct critnib_node *m;
		if ((m = n->child[i]))
			return is_leaf(m) ? m : any_leaf(m);
	}
	return NULL;
}

/*
 * critnib_set -- insert a new entry
 */
int
critnib_set(struct critnib *c, struct cache_entry *e)
{
	struct critnib_leaf *k = alloc_leaf(c);
	if (!k)
		return ENOMEM;

	const char *key = (void *)&e->key;
	byten_t key_len = (byten_t)(e->key.ksize + sizeof(e->key.ksize));
	k->key = key;
	k->key_len = key_len;
	k->value = e;
	k = (void *)((uint64_t)k | 1);

	os_mutex_unlock(&c->mutex);
	struct critnib_node *n = c->root;
	if (!n) {
		c->root = (void *)k;
		os_mutex_unlock(&c->mutex);
		return 0;
	}

	/*
	 * Need to descend the tree twice: first to find a leaf that
	 * represents a subtree whose all keys share a prefix at least as
	 * long as the one common to the new key and that subtree.
	 */
	while (!is_leaf(n) && n->byte < key_len) {
		struct critnib_node *nn =
			n->child[slice_index(key[n->byte], n->bit)];
		if (nn)
			n = nn;
		else {
			n = any_leaf(n);
			break;
		}
	}

	ASSERT(n);
	if (!is_leaf(n))
		n = any_leaf(n);

	ASSERT(n);
	ASSERT(is_leaf(n));
	struct critnib_leaf *nk = to_leaf(n);

	/* Find the divergence point, accurate to a byte. */
	byten_t common_len = (nk->key_len < (byten_t)key_len)
			    ? nk->key_len : (byten_t)key_len;
	byten_t diff;
	for (diff = 0; diff < common_len; diff++) {
		if (nk->key[diff] != key[diff])
			break;
	}

	if (diff >= common_len) {
		// update or conflict between keys being a prefix of each other
		return EEXIST;
	}

	/* Calculate the divergence point within the single byte. */
	char at = nk->key[diff] ^ key[diff];
	bitn_t sh = util_mssb_index((uint32_t)at) & (bitn_t)~(SLICE - 1);

	/* Descend into the tree again. */
	n = c->root;
	struct critnib_node **parent = &c->root;
	while (n && !is_leaf(n) &&
	       (n->byte < diff || (n->byte == diff && n->bit >= sh))) {
		parent = &n->child[slice_index(key[n->byte], n->bit)];
		n = *parent;
	}

	/*
	 * If the divergence point is at same nib as an existing node, and
	 * the subtree there is empty, just place our leaf there and we're
	 * done.  Obviously this can't happen if SLICE == 1.
	 */
	if (!n) {
		*parent = (void *)k;
		os_mutex_unlock(&c->mutex);
		return 0;
	}

	/* If not, we need to insert a new node in the middle of an edge. */
	if (!(n = alloc_node(c))) {
		os_mutex_unlock(&c->mutex);
		free(k);
		return ENOMEM;
	}

	n->child[slice_index(nk->key[diff], sh)] = *parent;
	n->child[slice_index(key[diff], sh)] = (void *)k;
	n->byte = diff;
	n->bit = sh;
	*parent = n;
	os_mutex_unlock(&c->mutex);
	return 0;
}

/*
 * critnib_get -- query a key
 */
void *
critnib_get(struct critnib *c, const struct cache_entry *e)
{
        const char *key = (void *)&e->key;
        byten_t key_len = (byten_t)(e->key.ksize + sizeof(e->key.ksize));

	os_mutex_unlock(&c->mutex);
	struct critnib_node *n = c->root;
	while (n && !is_leaf(n)) {
		if (n->byte >= key_len)
			return NULL;
		n = n->child[slice_index(key[n->byte], n->bit)];
	}

	if (!n)
		return NULL;

	struct critnib_leaf *k = to_leaf(n);

	/*
	 * We checked only nibs at divergence points, have to re-check the
	 * whole key.
	 */
	if (key_len != k->key_len || memcmp(key, k->key, key_len))
		return NULL;
	return k->value;
}

/*
 * critnib_remove -- query and delete a key
 *
 * Neither the key nor its value are freed, just our private nodes.
 */
void *
critnib_remove(struct critnib *c, const struct cache_entry *e)
{
        const char *key = (void *)&e->key;
        byten_t key_len = (byten_t)(e->key.ksize + sizeof(e->key.ksize));

	os_mutex_unlock(&c->mutex);
	struct critnib_node **pp = NULL;
	struct critnib_node *n = c->root;
	struct critnib_node **parent = &c->root;

	/* First, do a get. */
	while (n && !is_leaf(n)) {
		if (n->byte >= key_len) {
			os_mutex_unlock(&c->mutex);
			return NULL;
		}
		pp = parent;
		parent = &n->child[slice_index(key[n->byte], n->bit)];
		n = *parent;
	}

	if (!n) {
		os_mutex_unlock(&c->mutex);
		return NULL;
	}

	struct critnib_leaf *k = to_leaf(n);
	if (key_len != k->key_len || memcmp(key, k->key, key_len)) {
		os_mutex_unlock(&c->mutex);
		return NULL;
	}

	void *value = k->value;

	/* Remove the entry (leaf). */
	*parent = NULL;
	free(k);

	if (!pp) { /* was root */
		os_mutex_unlock(&c->mutex);
		return value;
	}

	/* Check if after deletion the node has just a single child left. */
	n = *pp;
	struct critnib_node *only_child = NULL;
	for (int i = 0; i < SLNODES; i++) {
		if (n->child[i]) {
			if (only_child) {
				/* Nope. */
				os_mutex_unlock(&c->mutex);
				return value;
			}
			only_child = n->child[i];
		}
	}

	/* Yes -- shorten the tree's edge. */
	ASSERT(only_child);
	*pp = only_child;
	free(n);
	os_mutex_unlock(&c->mutex);
	return value;
}
