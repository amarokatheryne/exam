-- ============================================================
-- TABLAS A RECONSTRUIR
-- Necesario si cambia particion/clustering.
-- RAW DESDE LANDING
-- ============================================================

-- Primero se eliminan las tablas para evitar errores si cambia la particion o el clustering.

-- 01 homework
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.homework`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.homework`
  PARTITION BY DATE(created)
  CLUSTER BY id, section_id, teacher_id
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.homework`;

-- 02 assignment
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.assignment`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.assignment`
  PARTITION BY DATE(created)
  CLUSTER BY homework_id, student_id, progress, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.assignment`;

-- 03 connection_course
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_learning.connection_course`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_learning.connection_course`
  PARTITION BY DATE(created)
  CLUSTER BY section_id, user_id, user_role, week_number
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_learning.connection_course`;

-- 04 enrollment
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_learning.enrollment`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_learning.enrollment`
  PARTITION BY DATE(created)
  CLUSTER BY section_id, user_id, course_id
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_learning.enrollment`;

-- 05 forum
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.forum`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.forum`
  PARTITION BY DATE(created)
  CLUSTER BY id, course_id
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.forum`;

-- 06 content
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.content`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.content`
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.content`;

-- 07 quiz
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.quiz`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.quiz`
  PARTITION BY DATE(created)
  CLUSTER BY evaluation_id, student_id, progress, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.quiz`;

-- 08 theme
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.theme`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.theme`
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.theme`;

-- 09 evaluation
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.evaluation`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.evaluation`
  PARTITION BY DATE(created)
  CLUSTER BY id, section_id, course_id, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.evaluation`;

-- 10 section
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.section`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.section`
  PARTITION BY DATE(created)
  CLUSTER BY id, course_id, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.section`;

-- 11 forum_evaluation
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.forum_evaluation`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.forum_evaluation`
  PARTITION BY DATE(created)
  CLUSTER BY forum_id, user_id, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.forum_evaluation`;

-- 12 academic_period
DROP TABLE IF EXISTS `dev-utpbi-data-operation.raw_pao_course.academic_period`;
CREATE TABLE `dev-utpbi-data-operation.raw_pao_course.academic_period`
  PARTITION BY DATE(created)
  CLUSTER BY id, course_id, status
AS
SELECT
  *,
  CURRENT_TIMESTAMP() AS raw_load_ts,
  CURRENT_DATETIME('America/Lima') AS raw_load_datetime_lima
FROM `dev-utpbi-data-operation.landing_pao_course.academic_period`;
