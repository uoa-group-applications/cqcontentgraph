--
-- Table structure for table `Node`
--

DROP TABLE IF EXISTS `Node`;
CREATE TABLE `Node` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `parent_id` int(11) DEFAULT NULL,
  `site` varchar(100) DEFAULT NULL,
  `path` varchar(1024) DEFAULT NULL,
  `sub` varchar(1024) DEFAULT NULL,
  `resourceType` varchar(512) DEFAULT NULL,
  `type` varchar(100) DEFAULT NULL,
  `title` text,
  PRIMARY KEY (`id`),
  KEY `NodeTypePathMapping` (`resourceType`(100),`path`(200)),
  KEY `SiteTypeMapping` (`site`,`type`,`resourceType`(100))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `Property`
--

DROP TABLE IF EXISTS `Property`;
CREATE TABLE `Property` (
  `sub` varchar(1024) DEFAULT NULL,
  `value` text,
  `nodeId` int(11) DEFAULT NULL,
  `name` varchar(100) DEFAULT NULL,
  `path` varchar(1024) DEFAULT NULL,
  KEY `PropertyNamePathMapping` (`path`(200),`name`),
  KEY `PropertyNameIdMapping` (`nodeId`,`name`),
  KEY `PropertyNameValueDescMapping` (`name`,`value`(200)),
  KEY `PropertyNameValueAscMapping` (`name`,`value`(200))
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

--
-- Table structure for table `SynchState`
--

DROP TABLE IF EXISTS `SynchState`;
CREATE TABLE `SynchState` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `state` enum('reindexing','disabled','operational','update','periodic_update') DEFAULT NULL,
  `msg` varchar(1024) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

