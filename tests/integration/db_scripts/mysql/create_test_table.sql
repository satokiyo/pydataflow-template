DROP TABLE IF EXISTS `test`;
CREATE TABLE IF NOT EXISTS `test`
(
  `id`               INT(20) AUTO_INCREMENT,
  `name`             VARCHAR(20) NOT NULL,
  `date`             DATE NOT NULL,
  `memo`             VARCHAR(50),
  PRIMARY KEY (`id`, `date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
;

START TRANSACTION;
INSERT INTO
    test(name, date, memo)
VALUES
    ('test data1', '2023-01-01', 'memo1'),
    ('test data2', '2023-02-02', NULL),
    ('test data3', '2023-03-03', 'memo3'),
    ('test data4', '2023-04-04', NULL),
    ('test data5', '2023-05-05', NULL);
COMMIT;