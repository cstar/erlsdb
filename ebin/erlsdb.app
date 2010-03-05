{application, erlsdb,
 [{description, "Access to SimpleDB"},
  {author, "Eric Cestari"},
  {vsn, "1.5"},
  {modules, [cachedsdb, erlsdb, erlsdb_server, erlsdb_sup, erlsdb_util, sdb_test]},
  {mod, {erlsdb,[]}},
  {registered, [erlsdb_sup]},
  {applications, [kernel, stdlib, sasl, crypto, ibrowse, xmerl]}
 ]}.
