package support

import (
	"bytes"
	"fmt"
	"testing"
)

func TestGenSystab(t *testing.T) {
	tests := []struct {
		name string
		def  ScriptDef
		exp  string
	}{
		{
			name: "dolt history",
			def: ScriptDef{
				Name:     "history",
				Query:    "select count(*) from dolt_history_xy where commit_hash = (select commit_hash from dolt_log limit 1 offset 2)",
				Branches: 0,
				Commits:  2,
				Dummy: map[string]string{
					"dolt_history_xy": "dh_xy",
					"dolt_log":        "dl",
				},
				DiffsPerCommit: 3,
			},
			exp: `function prepare()
  sysbench.opt.threads = 1
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  con:query([[
create table dh_xy (
  commit_hash varchar(32),
  committer varchar(20),
  commit_date datetime,
  x int,
  y varchar(20),
  index (commit_hash)
)
  ]])
  con:query([[
insert into dh_xy values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 0, 'row 0'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 1, 'row 1'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 2, 'row 2');
]])
  con:query([[
insert into dh_xy values
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 0, 'row 0'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 1, 'row 1'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 2, 'row 2'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 3, 'row 3'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 4, 'row 4'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 5, 'row 5');
]])
  con:query([[
create table dl (
  commit_hash varchar(32),
  committer varchar(20),
  date datetime,
  email varchar(100),
  message varchar(100),
  primary key (commit_hash)
)
  ]])
  con:query([[
insert into dl values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 'max@dolthub.com', 'a commit message'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 'max@dolthub.com', 'a commit message');
]])
`,
		},
		{
			name: "dolt diff",
			def: ScriptDef{
				Name:     "history",
				Query:    "select * from dd where commit_hash = (select commit_hash from dl limit 1 offset 599)",
				Branches: 0,
				Commits:  2,
				Dummy: map[string]string{
					"dolt_diff": "dd",
					"dolt_log":  "dl",
				},
				DiffsPerCommit: 3,
			},
			exp: `function prepare()
  sysbench.opt.threads = 1
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  con:query([[
create table dh_xy (
  commit_hash varchar(32),
  committer varchar(20),
  commit_date datetime,
  x int,
  y varchar(20),
  index (commit_hash)
)
  ]])
  con:query([[
insert into dh_xy values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 0, 'row 0'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 1, 'row 1'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 2, 'row 2');
]])
  con:query([[
insert into dh_xy values
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 0, 'row 0'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 1, 'row 1'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 2, 'row 2'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 3, 'row 3'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 4, 'row 4'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 5, 'row 5');
]])
  con:query([[
create table dl (
  commit_hash varchar(32),
  committer varchar(20),
  date datetime,
  email varchar(100),
  message varchar(100),
  primary key (commit_hash)
)
  ]])
  con:query([[
insert into dl values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 'max@dolthub.com', 'a commit message'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T11:55:17-08:00', 'max@dolthub.com', 'a commit message');
]])
`,
		},
		{
			name: "dolt diff log join",
			def: ScriptDef{
				Name:     "dolt diff log join",
				Query:    "select * from dd_xy join dl on commit_hash = to_commit\"",
				Branches: 0,
				Commits:  2,
				Dummy: map[string]string{
					"dolt_diff_xy": "dd_xy",
					"dolt_log":     "dl",
				},
				DiffsPerCommit: 3,
			},
			exp: `function prepare()
  sysbench.opt.threads = 1
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  con:query([[
create table dd_xy (
  from_commit varchar(32),
  from_commit_date datetime,
  to_commit varchar(32),
  to_commit_date datetime,
  diff_type varchar(20),
  to_x int,
  to_y varchar(20),
  from_x int,
  from_y varchar(20),
  index (from_commit),
  index (to_commit)
  )
]])
  con:query([[
insert into dd_xy values
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 0, 'row 0', NULL, NULL),
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 1, 'row 1', 0, row 0),
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 2, 'row 2', 1, row 1);
]])
  con:query([[
insert into dd_xy values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 3, 'row 3', , ''),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 4, 'row 4', 3, 'row 3'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 5, 'row 5', 4, 'row 4');
]])
  con:query([[
create table dl (
  commit_hash varchar(32),
  committer varchar(20),
  date datetime,
  email varchar(100),
  message varchar(100),
  primary key (commit_hash)
)
  ]])
  con:query([[
insert into dl values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T12:39:48-08:00', 'max@dolthub.com', 'a commit message'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T12:39:48-08:00', 'max@dolthub.com', 'a commit message');
]])
end
`,
		},
		{
			name: "dolt commit ancestors",
			def: ScriptDef{
				Name:     "dca",
				Query:    "select * from dca where commit_hash = (select commit_hash from dolt_log limit 1 offset 599)",
				Branches: 0,
				Commits:  2,
				Dummy: map[string]string{
					"dolt_commit_ancestors": "dca",
					"dolt_log":              "dl",
				},
				DiffsPerCommit: 3,
			},
			exp: `function prepare()
  sysbench.opt.threads = 1
  local drv = sysbench.sql.driver()
  local con = drv:connect()
  con:query([[
create table dd_xy (
  from_commit varchar(32),
  from_commit_date datetime,
  to_commit varchar(32),
  to_commit_date datetime,
  diff_type varchar(20),
  to_x int,
  to_y varchar(20),
  from_x int,
  from_y varchar(20),
  index (from_commit),
  index (to_commit)
  )
]])
  con:query([[
insert into dd_xy values
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 0, 'row 0', NULL, NULL),
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 1, 'row 1', 0, row 0),
  (NULL, NULL, 'gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', 'added', 2, 'row 2', 1, row 1);
]])
  con:query([[
insert into dd_xy values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 3, 'row 3', , ''),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 4, 'row 4', 3, 'row 3'),
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', '2022-11-23T12:39:48-08:00', '2lyghvflnk93nwie9s2vtopzxd8u1ap6', '2022-11-23T12:39:48-08:00', 'added', 5, 'row 5', 4, 'row 4');
]])
  con:query([[
create table dl (
  commit_hash varchar(32),
  committer varchar(20),
  date datetime,
  email varchar(100),
  message varchar(100),
  primary key (commit_hash)
)
  ]])
  con:query([[
insert into dl values
  ('gcwogskflpeg7yievuh1kqdnwrwnz5vd', 'Max Hoffman', '2022-11-23T12:39:48-08:00', 'max@dolthub.com', 'a commit message'),
  ('2lyghvflnk93nwie9s2vtopzxd8u1ap6', 'Max Hoffman', '2022-11-23T12:39:48-08:00', 'max@dolthub.com', 'a commit message');
]])
end
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gen ScriptGen
			var buf bytes.Buffer
			gen.w = &buf
			gen.define = tt.def
			gen.genPrepareDummy(tt.def)
			//gen.Generate(tt.def, &buf)
			fmt.Println(buf.String())
		})
	}
}
