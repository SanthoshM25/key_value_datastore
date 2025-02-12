CREATE TABLE users ( 
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(64) NOT NULL UNIQUE KEY,
    password VARCHAR(72) NOT NULL
);

CREATE TABLE quotas (
    user_id INT PRIMARY KEY, 
    provisioned INT NOT NULL, 
    utilised INT NOT NULL
);

CREATE TABLE data_store (
    user_id INT, 
    data_key VARCHAR(32), 
    data_value JSON, 
    ttl INT, 
    PRIMARY KEY(user_id, data_key)
);

CREATE EVENT clean_expired_data
ON SCHEDULE EVERY 1 DAY
DO
  DELETE FROM data_store
  WHERE ttl != 0 AND ttl < UNIX_TIMESTAMP(NOW());