struct critnib;

struct critnib *critnib_new(void);
void critnib_delete(struct critnib *c);
int critnib_set(struct critnib *c, const char *key, size_t key_len,
		void *value);
const void *critnib_get(struct critnib *c, const char *key, size_t key_len);
void *critnib_remove(struct critnib *c, const char *key, size_t key_len);
