create extension bztree;

create table t(pk integer, val integer);

create index on t using bztree(pk);

insert into t values (generate_series(1,1000000),0);

analyze t;

select * from t where pk=1000;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) select * from t where pk=1000;

update t set val=val+1 where pk=10;
explain (COSTS OFF, TIMING OFF, SUMMARY OFF) update t set val=10 where pk=10;
select * from t where pk=10;

delete from t where pk=100;
select * from t where pk=100;
vacuum t;
select * from t where pk=10;
select * from t where pk=99;
select * from t where pk=100;
select * from t where pk=101;

drop table t;
