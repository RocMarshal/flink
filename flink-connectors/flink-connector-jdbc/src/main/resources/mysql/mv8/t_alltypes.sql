

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for t_alltypes
-- Server version: 8.0.20 MySQL Community Server - GPL
-- ----------------------------
DROP TABLE IF EXISTS `t_alltypes`;
CREATE TABLE `t_alltypes` (
    `t1` bigint unsigned NOT NULL AUTO_INCREMENT,
    `t1_1` bigint DEFAULT NULL,
    `t2` binary(255) DEFAULT NULL,
    `t3` bit(33) DEFAULT NULL,
    `t4` blob,
    `t5` char(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci DEFAULT NULL,
    `t6` date DEFAULT NULL,
    `t7` datetime DEFAULT NULL,
    `t8` decimal(2,0) DEFAULT NULL,
    `t8_1` decimal(2,0) unsigned DEFAULT NULL,
    `t9` double(1,0) DEFAULT NULL,
  `t9_1` double(1,0) unsigned DEFAULT NULL,
  `t10` enum('a') DEFAULT NULL,
  `t11` float DEFAULT NULL,
  `t11_1` float unsigned DEFAULT NULL,
  `t12` geometry DEFAULT NULL,
  `t13` geomcollection DEFAULT NULL,
  `t14` int DEFAULT NULL,
  `t14_1` int unsigned DEFAULT NULL,
  `t15` int DEFAULT NULL,
  `t15_1` int unsigned DEFAULT NULL,
  `t16` json DEFAULT NULL,
  `t17` linestring DEFAULT NULL,
  `t18` longblob,
  `t19` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci,
  `t20` mediumblob,
  `t21` mediumint DEFAULT NULL,
  `t21_1` mediumint unsigned DEFAULT NULL,
  `t22` mediumtext,
  `t23` multilinestring DEFAULT NULL,
  `t24` multipoint DEFAULT NULL,
  `t25` multipolygon DEFAULT NULL,
  `t26` decimal(2,0) DEFAULT NULL,
  `t26_1` decimal(2,0) unsigned DEFAULT NULL,
  `t27` point DEFAULT NULL,
  `t28` polygon DEFAULT NULL,
  `t29` double(255,0) DEFAULT NULL,
  `t29_1` double(255,0) unsigned DEFAULT NULL,
  `t30` set('bbb') DEFAULT NULL,
  `t31` smallint DEFAULT NULL,
  `t31_1` smallint unsigned DEFAULT NULL,
  `t32` text,
  `t33` time DEFAULT NULL,
  `t34` timestamp NULL DEFAULT NULL,
  `t35` tinyblob,
  `t36` tinyint DEFAULT NULL,
  `t36_1` tinyint unsigned DEFAULT NULL,
  `t37` tinytext,
  `t38` varbinary(255) DEFAULT NULL,
  `t39` varchar(255) DEFAULT NULL,
  `t40` year DEFAULT NULL,
  PRIMARY KEY (`t1`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SET FOREIGN_KEY_CHECKS = 1;
