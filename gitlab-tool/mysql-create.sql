/*
 Navicat Premium Data Transfer

 Source Server         : localhost
 Source Server Type    : MySQL
 Source Server Version : 50720
 Source Host           : 127.0.0.1:3306
 Source Schema         : etl

 Target Server Type    : MySQL
 Target Server Version : 50720
 File Encoding         : 65001

 Date: 18/09/2018 15:56:37
*/

SET NAMES utf8mb4;
SET FOREIGN_KEY_CHECKS = 0;

-- ----------------------------
-- Table structure for etl_lock
-- ----------------------------
DROP TABLE IF EXISTS `etl_lock`;
CREATE TABLE `etl_lock` (
  `schema_name` varchar(30) NOT NULL ,
  `table_name` varchar(30) NOT NULL,
  `date_str` varchar(30) NOT NULL,
  `is_lock` int(2) NOT NULL,
PRIMARY KEY (`schema_name`,`table_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

SET FOREIGN_KEY_CHECKS = 1;
