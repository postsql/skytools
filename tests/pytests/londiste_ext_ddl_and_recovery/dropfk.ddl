
    ALTER TABLE pgbench_tellers DROP CONSTRAINT pgbench_tellers_branches_fk ;
    ALTER TABLE pgbench_accounts DROP CONSTRAINT pgbench_accounts_branches_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_branches_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_tellers_fk;
    ALTER TABLE pgbench_history DROP CONSTRAINT pgbench_history_accounts_fk;
