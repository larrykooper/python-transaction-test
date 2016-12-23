BEGIN;
-- do some updates
INSERT INTO employee (lastname, departmentid)
  VALUES ('Max', 1234);
UPDATE employee
  SET departmentid = 570
  WHERE lastname = 'Comcast/Clayton, GA';