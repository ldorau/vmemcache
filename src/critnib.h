#include "vmemcache.h"

struct critnib;

struct critnib *critnib_new(void);
void critnib_delete(struct critnib *c);
int critnib_set(struct critnib *c, struct cache_entry *e);
void *critnib_get(struct critnib *c, const struct cache_entry *e);
void *critnib_remove(struct critnib *c, const struct cache_entry *e);
