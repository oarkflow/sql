# SQL Expression Examples

## Basic SELECT
```sql
SELECT * FROM read_service('my_table');
```

## SELECT with WHERE condition
```sql
SELECT id, name
FROM read_service('users')
WHERE age >= 18;
```

## String Functions
```sql
SELECT CONCAT(first_name, ' ', last_name) AS full_name
FROM read_service('employees');
```

## Conditional Function (IF)
```sql
SELECT IF(age >= 18, 'Adult', 'Minor') AS status
FROM read_service('users');
```

## Substring Function (SUBSTR)
```sql
SELECT SUBSTR(name, 1, 3) AS short_name
FROM read_service('products');
```

## Aggregate Functions
```sql
SELECT COUNT(*) AS total, AVG(salary) AS avg_salary
FROM read_service('employees');
```

## Date Functions (TO_DATE, DATEDIFF)
```sql
SELECT TO_DATE('2023-01-01', 'YYYY-MM-DD') AS start_date,
       DATEDIFF('2023-01-10', '2023-01-01') AS day_diff;
```

## Logical Operators and LIKE (with case‑insensitive mode)
```sql
SELECT *
FROM read_service('articles')
WHERE title LIKE 'i:%Go%' AND published = 'true';
```

## CASE Expression
```sql
SELECT
  CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    ELSE 'C'
  END AS grade
FROM read_service('exams');
```

## JOIN Example
```sql
SELECT p.*, c.comment
FROM read_service('posts') AS p
JOIN read_service('comments') AS c ON p.id = c.postId;
```

## Compound Query (UNION ALL)
```sql
SELECT id, name FROM read_service('customers')
UNION ALL
SELECT id, name FROM read_service('clients');
```
