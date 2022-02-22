DROP DATABASE IF EXISTS MyDB;
CREATE DATABASE MyDB;

USE MyDB;

CREATE TABLE admin_accounts (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, username VARCHAR(31), password VARCHAR(31));
INSERT INTO admin_accounts (username, password) VALUES
("admin", "password");

CREATE TABLE user_accounts (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, username VARCHAR(31), password VARCHAR(31));
INSERT INTO user_accounts (username, password) VALUES ("user","password");

SELECT * FROM user_accounts;
SELECT * FROM admin_accounts;

SELECT COUNT(*) FROM admin_accounts WHERE username = 'admin' AND password = 'password';

SELECT COUNT(*) FROM user_accounts WHERE username = 'username' AND password = 'password';

DELETE FROM admin_accounts WHERE username = 'username' AND password = 'password';
DELETE FROM user_accounts WHERE username = 'username' AND password = 'password';

UPDATE admin_accounts SET username = 'username', password = 'password' WHERE password = 'oldPassword';

UPDATE user_accounts SET username = 'username', password = 'password' WHERE password = 'oldPassword'

