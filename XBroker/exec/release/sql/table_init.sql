SET @table_user_seed_salt = RANDOM_BYTES(16);

INSERT INTO table_user (`user`, password_hash, password_salt, status)
VALUES ('fengwanggang', UNHEX(SHA2(CONCAT(@table_user_seed_salt, '123456'), 256)), @table_user_seed_salt, 1)
ON DUPLICATE KEY UPDATE user_id = user_id;

SET @table_user_seed_salt = NULL;

SET @table_account_seed_salt = RANDOM_BYTES(16);

INSERT INTO table_account (broker_id, account, password_hash, password_salt, initial_balance_cents, status)
VALUES (1, 'fengwanggang', UNHEX(SHA2(CONCAT(@table_account_seed_salt, '123456'), 256)), @table_account_seed_salt, 100000000, 1)
ON DUPLICATE KEY UPDATE broker_id = broker_id;

SET @table_account_seed_salt = NULL;
