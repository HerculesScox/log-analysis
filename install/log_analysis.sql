-- phpMyAdmin SQL Dump
-- version 4.4.6
-- http://www.phpmyadmin.net
--
-- Host: localhost
-- Generation Time: May 27, 2015 at 03:43 AM
-- Server version: 5.5.43
-- PHP Version: 5.4.40

SET SQL_MODE = "NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";


/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `log_analysis`
--
CREATE DATABASE IF NOT EXISTS `log_analysis` DEFAULT CHARACTER SET utf8 COLLATE utf8_general_ci;
USE `log_analysis`;

-- --------------------------------------------------------

--
-- Table structure for table `table_job`
--

CREATE TABLE IF NOT EXISTS `table_job` (
  `jobid` varchar(50) NOT NULL,
  `workflowID` varchar(200) NOT NULL,
  `workflowNode` varchar(50) NOT NULL,
  `logPath` varchar(300) NOT NULL,
  `detailInfo` text NOT NULL,
  `remark` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `table_query`
--

CREATE TABLE IF NOT EXISTS `table_query` (
  `queryString` text NOT NULL,
  `workflowID` varchar(200) NOT NULL,
  `jobDependency` varchar(400) NOT NULL,
  `username` varchar(50) NOT NULL,
  `launchtime` varchar(100) NOT NULL,
  `remark` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- --------------------------------------------------------

--
-- Table structure for table `table_task`
--

CREATE TABLE IF NOT EXISTS `table_task` (
  `taskid` varchar(50) NOT NULL,
  `jobid` varchar(50) NOT NULL,
  `taskType` varchar(20) NOT NULL,
  `logPath` varchar(200) NOT NULL,
  `detailInfo` text NOT NULL,
  `remark` varchar(200) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
