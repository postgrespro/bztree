create extension bztree;

create table t(pk integer, val integer);

create unique index on t using bztree(pk);

insert into t values (generate_series(1,1000000),0);

analyze t;

select * from t where pk=1000;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t where pk=1000;

update t set val=10 where pk=10;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) update t set val=10 where pk=10;
select * from t where pk=10;

insert into t values (1,1) on conflict(pk) do update set val=t.val;
select * from t where pk=1;

delete from t where pk=10;
select * from t where pk=10;
vacuum t;
select * from t where pk in (9,10,11);
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t where pk in (9,10,11);

drop table t;
