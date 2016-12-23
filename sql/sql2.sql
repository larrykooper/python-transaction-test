-- do some things

DELETE FROM employee
WHERE departmentid = 0;
ROLLBACK;