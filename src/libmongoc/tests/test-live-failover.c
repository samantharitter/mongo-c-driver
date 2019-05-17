#include <mongoc/mongoc.h>

#include "json-test.h"
#include "test-libmongoc.h"
#include "mongoc/mongoc-client.h"
#include "mongoc/mongoc-collection.h"

typedef bool (*_workload_fn_t) (mongoc_client_t *client, void *ctx);

static void
run_scenario (_workload_fn_t workload,
	      void *workload_ctx,
	      const char *python_path,
	      const char *config_path)
{
   int link[2];
   pid_t pid;

   /* Pipe the python script's output here */
   BSON_ASSERT (pipe(link) != -1);
   
   pid = fork();

   /* Failed to fork */
   BSON_ASSERT (pid != -1);

   if (pid > 0) {
      int buf_max = 4096;
      char buf[buf_max];
      int status;
      int bytes_read;

      /* In the parent, run the workload */

      /* Read piped input from the child */
      close(link[1]);

      while (waitpid (pid, &status, WNOHANG) == 0) {
	 sleep(1);

	 bytes_read = read(link[0], buf, buf_max);
	 if (bytes_read > 0) {
	    fprintf(stderr, "%.*s\n", bytes_read, buf);
	 } if (bytes_read == -1) {
	    fprintf(stderr, "Error :(\n");
	 } else {
	 }
      }

      fprintf (stderr, "child exited, status %d\n", status);
   } else {
      /* In the child, send our output back to the parent */
      while ((dup2(link[1], STDOUT_FILENO) == -1) &&
	     (errno == EINTR)) {
	 /* Repeat in case of signal interruption */
      }
      close (link[0]);
      close (link[1]);

      /* run the scenario */
      execl (python_path, "topology", config_path, "--sleep", "1", NULL);

      /* exec should never return */
      fprintf (stderr, "Error: exec failed\n");
      BSON_ASSERT (false);
   }

}

static void
test_workload (_workload_fn_t workload, void *workload_ctx)
{
   int i;
   int num_configs;
   char dir_path[PATH_MAX];
   char script_path[PATH_MAX];
   char config_paths[MAX_NUM_TESTS][MAX_TEST_NAME_LENGTH];

   /* Load in the python script */
   test_framework_resolve_path (JSON_DIR "/live_failover/scenario.py", script_path);
   
   /* Load in all the config files */
   test_framework_resolve_path (JSON_DIR "/live_failover/configs", dir_path);
   num_configs = collect_tests_from_dir (&config_paths[0],
					 dir_path,
					 0,
					 MAX_NUM_TESTS);

   /* For each config file, run scenario */
   for (i = 0; i < num_configs; i++) {
      run_scenario (workload,
		    workload_ctx,
		    script_path,
		    config_paths[i]);
   }
}

typedef struct {
   bson_t *selector;
   bson_t *update;
   bson_t reply;
   bson_error_t error;
} update_one_ctx_t;

static bool
_run_update_one (mongoc_client_t *client, void *ctx)
{
   mongoc_collection_t *coll;
   update_one_ctx_t *update_ctx;

   update_ctx = (update_one_ctx_t *)ctx;

   coll = mongoc_client_get_collection (client, "test", "test");
   if (!coll) {
      return false;
   }

   if (!mongoc_collection_update_one (coll,
				      update_ctx->selector,
				      update_ctx->update,
				      NULL,
				      &update_ctx->reply,
				      &update_ctx->error)) {
      return false;
   }

   return true;
}

static void
test_update_one_workload (void)
{
   update_one_ctx_t ctx;

   ctx.selector = BCON_NEW ("_id", BCON_INT32 (1));
   ctx.update = BCON_NEW ("$set", "{", "updated", BCON_BOOL (true), "}");

   test_workload (_run_update_one, &ctx);

   bson_destroy (ctx.selector);
   bson_destroy (ctx.update);
}

void
test_live_failover_install (TestSuite *suite)
{
   TestSuite_Add (suite,
		  "/live_failover/update_one",
		  test_update_one_workload);
}
