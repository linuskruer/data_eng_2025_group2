-- Project 3: Grant necessary permissions to default user for role/user management.
-- Run this FIRST before 03_roles_and_grants.sql

-- Grant role management permissions to default user
GRANT CREATE ROLE ON *.* TO default;
GRANT CREATE USER ON *.* TO default;
GRANT ROLE ADMIN ON *.* TO default;
GRANT ALL ON *.* TO default WITH GRANT OPTION;

