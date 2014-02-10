-ifndef(ISQL_HRL).
-define(ISQL_HRL, "isql.hrl").

-record(sql_entity, {
          host,
          port,
          username,
          password,
          database,
          encoding
}).

-record(lb_pid, {entity, pid}).

-record(client_conn, {key, cur_pipeline_size = 0, reqs_served = 0}).

-record(isql_conf, {key, value}).

-endif.
