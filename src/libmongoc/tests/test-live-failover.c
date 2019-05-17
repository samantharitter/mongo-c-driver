#include <mongoc/mongoc.h>

#include "json-test.h"
#include "test-libmongoc.h"
#include "mongoc/mongoc-client.h"
#include "mongoc/mongoc-collection.h"

static pid_t
launch_python_script (const char *python_path,
		      char *config_path,
		      int link[],
		      char *argv[])
{
   int status;
   pid_t pid;

   /* Pipe the python script's output here */
   BSON_ASSERT (pipe(link) != -1);

   pid = fork();
   BSON_ASSERT (pid != -1);

   if (pid > 0) {
      /* In the parent, read piped info from the child */
      close (link[1]);

      if (waitpid (pid, &status, WNOHANG) != 0) {
	 fprintf (stderr, "Error: child exited early with status %d\n", status);
	 BSON_ASSERT (false);
      }

      return pid;

   } else {
      /* In the child, send our output back to the parent */
      while ((dup2(link[1], STDOUT_FILENO) == -1) &&
	     (errno == EINTR)) {
	 /* Repeat in case of signal interruption */
      }
      close (link[0]);
      close (link[1]);

      execv (python_path, argv);

      fprintf (stderr, "Error: exec failed\n");
      BSON_ASSERT (false);
   }

}

static bool
monitor_script (pid_t pid, int link[], bool forever)
{
   int buf_max = 4096;
   char buf[buf_max];
   pid_t wait_res;
   int status;
   int bytes_read;

   /* To "continuously" print output, each time we
      check the process status, read some input */
   do {
      fprintf (stderr, "monitoring\n");
      bytes_read = read(link[0], buf, buf_max);
      if (bytes_read > 0) {
	 fprintf(stderr, "%.*s\n", bytes_read, buf);
      }

      wait_res = waitpid (pid, &status, WNOHANG);
      BSON_ASSERT (wait_res != -1);
      if (wait_res > 0) {
	 if (WIFEXITED(status)) {
	    fprintf (stderr, "child exited, status %d\n", status);
	    return false;
	 }
      }
      fprintf (stderr, "child still running\n");
   } while (forever);

   return true;
}


static bool
script_still_running (pid_t pid, int link[])
{
   return monitor_script (pid, link, false);
}


static void
wait_for_script_exit (pid_t pid, int link[])
{
   fprintf (stderr, "waiting for script to exit");
   monitor_script (pid, link, true);
}


typedef bool (*_workload_fn_t) (mongoc_client_t *client, void *ctx);

static void
run_scenario (_workload_fn_t workload,
	      void *workload_ctx,
	      const char *python_path,
	      char *config_path)
{
   mongoc_client_t *client;
   int link[2];
   pid_t pid;
   char *start_args[] = {"a", "start", config_path};
   char *scenario_args[] = {"a", "--sleep", "1", "scenario", config_path};
   char *stop_args[] = {"a", "stop", config_path};

   /* First, run script with the "start" command */
   pid = launch_python_script (python_path,
			       config_path,
			       link,
			       start_args);
   wait_for_script_exit (pid, link);

   /* TODO: dynamically create URI based on config file. */
   client = mongoc_client_new ("mongodb://localhost:5000,localhost:5001,localhost:5002/replicaSet=rs1");
   BSON_ASSERT (client);

   /* Next, run script with the "scenario" command. */
   pid = launch_python_script (python_path,
			       config_path,
			       link,
			       scenario_args);

   /* While the scenario is running, run our workload. */
   while (script_still_running (pid, link)) {
      BSON_ASSERT (workload (client, workload_ctx));
   }

   /* After the scenario is done, run our workload again. */
   BSON_ASSERT (workload (client, workload_ctx));

   /* Shut down the agent and cluster. */
   pid = launch_python_script (python_path,
			       config_path,
			       link,
			       stop_args);
   wait_for_script_exit (pid, link);
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
