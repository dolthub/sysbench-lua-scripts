-- Copyright (C) 2006-2018 Alexey Kopytov <akopytov@gmail.com>

-- This program is free software; you can redistribute it and/or modify
-- it under the terms of the GNU General Public License as published by
-- the Free Software Foundation; either version 2 of the License, or
-- (at your option) any later version.

-- This program is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
-- GNU General Public License for more details.

-- You should have received a copy of the GNU General Public License
-- along with this program; if not, write to the Free Software
-- Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA

dolt_prepare = prepare

function prepare()
    sysbench.opt.threads = 1
    drv = sysbench.sql.driver()
    con = drv:connect()
    create_table(drv, con, 1)
end

function thread_init()
    drv = sysbench.sql.driver()
    con = drv:connect()

    stmt = con:prepare('SELECT * FROM sbtest1 WHERE big_int_col > 0')
end

function thread_done()
    stmt:close()
    con:disconnect()
end

function event()
    stmt:execute()
end

function cleanup()
    local drv = sysbench.sql.driver()
    local con = drv:connect()
    print("Dropping table 'sbtest1'")
    con:query("DROP TABLE IF EXISTS sbtest1")
end

-- Command line options
sysbench.cmdline.options = {
    table_size = {"Number of rows per table", 10000},
    create_table_options = {"Extra CREATE TABLE options", ""},
}

function create_table(drv, con)
   local id_index_def, id_def
   local engine_def = ""
   local extra_table_options = ""
   local query

   print("Creating table 'sbtest1'...")

   query = [[
CREATE TABLE sbtest1 (
	id INT NOT NULL,
	tiny_int_col TINYINT NOT NULL,
	unsigned_tiny_int_col TINYINT UNSIGNED NOT NULL,
	small_int_col SMALLINT NOT NULL,
	unsigned_small_int_col SMALLINT UNSIGNED NOT NULL,
	medium_int_col MEDIUMINT NOT NULL,
	unsigned_medium_int_col MEDIUMINT UNSIGNED NOT NULL,
	int_col INT NOT NULL,
	unsigned_int_col INT UNSIGNED NOT NULL,
	big_int_col BIGINT NOT NULL,
	unsigned_big_int_col BIGINT UNSIGNED NOT NULL,
	decimal_col DECIMAL NOT NULL,
	float_col FLOAT NOT NULL,
	double_col DOUBLE NOT NULL,
	bit_col BIT NOT NULL,
	char_col CHAR NOT NULL,
	var_char_col VARCHAR(64) NOT NULL,
    tiny_text_col TINYTEXT NOT NULL,
    text_col TEXT NOT NULL,
    medium_text_col MEDIUMBLOB NOT NULL,
    long_text_col LONGTEXT NOT NULL,
    tiny_blob_col TINYBLOB NOT NULL,
    blob_col BLOB NOT NULL,
    medium_blob_col MEDIUMBLOB NOT NULL,
    long_blob_col LONGBLOB NOT NULL,
    json_col JSON NOT NULL,
    geom_col GEOMETRY NOT NULL,
	enum_col ENUM('val0', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10', 'val11', 'val12', 'val13') NOT NULL,
	set_col SET('val0', 'val1', 'val2', 'val3', 'val4', 'val5', 'val6', 'val7', 'val8', 'val9', 'val10', 'val11', 'val12', 'val13') NOT NULL,
	date_col DATE NOT NULL,
	time_col TIME NOT NULL,
	datetime_col DATETIME NOT NULL,
	timestamp_col TIMESTAMP NOT NULL,
	year_col YEAR NOT NULL,

	PRIMARY KEY(id),
	INDEX (big_int_col)
) ]]

   con:query(query)

    local query = [[INSERT INTO sbtest1
(id, tiny_int_col, unsigned_tiny_int_col, small_int_col, unsigned_small_int_col, medium_int_col, unsigned_medium_int_col, int_col, unsigned_int_col, big_int_col, unsigned_big_int_col, decimal_col, float_col, double_col, bit_col, char_col, var_char_col, tiny_text_col, text_col, medium_text_col, long_text_col, tiny_blob_col, blob_col, medium_blob_col, long_blob_col, json_col, geom_col, enum_col, set_col, date_col, time_col, datetime_col, timestamp_col, year_col)
VALUES
]]

    local str_vals = {"val0", "val1", "val2", "val3", "val4", "val5", "val6", "val7", "val8", "val9", "val10", "val11", "val12", "val13"}
    math.randomseed(0)

    con:bulk_insert_init(query)
    for i = 1, sysbench.opt.table_size do
        local row_values = "(" .. i .. "," ..                                             -- id
            math.random(-128, 127) .. "," ..                                   -- tiny_int_col
            math.random(0, 255) .. "," ..                                      -- unsigned_tiny_int_col
            math.random(-32768,  32767) .. "," ..                              -- small_int_col
            math.random(0, 65535) .. "," ..                                    -- unsigned_small_int_col
            math.random(-8388608, 8388607) .. "," ..                           -- medium_int_col
            math.random(0, 16777215) .. "," ..                                 -- unsigned_medium_int_col
            math.random(-2147483648, 2147483647) .. "," ..                     -- int_col
            math.random(0, 4294967295) .. "," ..                               -- unsigned_int_col
            math.random(-4611686018427387904, 4611686018427387903) .. "," ..   -- big_int_col
            math.random(0, 9223372036854775807) .. "," ..                      -- unsigned_big_int_col
            math.random() .. "," ..                                                    -- decimal_col
            math.random() .. "," ..                                                    -- float_col
            math.random() .. "," ..                                                    -- double_col
            math.random(0, 1) .. "," ..                                        -- bit_col
            "'" .. string.char(math.random(0x30, 0x5A)) .. "'" .. "," ..          -- char_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- var_char_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- tiny_text_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- text_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- medium_text_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- long_text_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- tiny_blob_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- blob_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- medium_blob_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- long_blob_col
            "'{\"" .. str_vals[math.random(1, 13)] .. "\":1}'" .. "," ..            -- json_col
            "ST_GeomFromText('Point(" .. math.random(1, 3) .. " " .. math.random(1,3) .. ")')" .. "," .. -- geom_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- enum_col
            "'" .. str_vals[math.random(1, 13)] .. "'" .. "," ..                -- set_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. "'" .. "," .. -- date_col
            "'0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- time_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. " 0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- datetime_col
            "'2020-0" .. math.random(1, 9) .. "-" .. math.random(10, 28) .. " 0" .. math.random(1, 9) .. ":" .. math.random(10, 59) .. ":00'" .. "," .. -- timestamp_col
            math.random(1901, 2155) .. ")"                                     -- year_col

        con:bulk_insert_next(row_values)
    end
    con:bulk_insert_done()
end
