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

## Advanced Nested/Array/JSON Queries
Assume service mappings:
- `read_service('users')` -> `https://jsonplaceholder.typicode.com/users`
- `read_service('posts')` -> `https://jsonplaceholder.typicode.com/posts`

### 1. Dot notation on nested objects
```sql
SELECT id, name, address.city, address.geo.lat
FROM read_service('users');
```

### 2. Quoted path segments for special keys
```sql
-- Example when key names contain special characters (like zip-code)
SELECT address."zip-code"
FROM read_service('users');
```

### 3. Bracket/index path syntax
```sql
-- Array index on primitive arrays
SELECT tags[0], tags[1]
FROM read_service('posts');

-- Array index on array of objects
SELECT education[0].year, education[1].year
FROM read_service('users');
```

### 4. Arrow path syntax (also supported)
```sql
SELECT id
FROM read_service('users')
WHERE education-> year > 2025;
```

### 5. `IN` with array fields
```sql
SELECT id, COUNT(*) AS total
FROM read_service('users')
WHERE tags IN ('mysql', 'db')
GROUP BY id;
```

### 6. Array operators: `ANY`, `ALL`, `CONTAINS`, `OVERLAPS`
```sql
-- Any score greater than 95
SELECT id
FROM read_service('users')
WHERE 95 < ANY(scores);

-- All scores greater than 70
SELECT id
FROM read_service('users')
WHERE 70 < ALL(scores);

-- tags contains single value
SELECT id
FROM read_service('users')
WHERE tags CONTAINS 'mysql';

-- tags overlaps with provided set
SELECT id
FROM read_service('users')
WHERE tags OVERLAPS ('mysql', 'db');
```

### 7. JSON path functions
```sql
SELECT
  JSON_EXTRACT(address, '$.city') AS city,
  JSON_EXISTS(address, '$.geo.lat') AS has_geo_lat,
  JSON_VALUE(address, '$.geo.lng') AS lng
FROM read_service('users');
```

### 8. Typed casts (`CAST` and `::`)
```sql
-- Function cast
SELECT id
FROM read_service('posts')
WHERE CAST(id AS int) > 50;

-- Operator cast
SELECT id
FROM read_service('posts')
WHERE id::int > 50;
```

### 9. LATERAL UNNEST for arrays of objects
```sql
SELECT u.id, e.year
FROM read_service('users') AS u
CROSS JOIN LATERAL UNNEST(u.education) AS e
WHERE e.year > 2020;
```

### 10. LATERAL UNNEST for primitive arrays
```sql
SELECT p.id, t.value AS tag
FROM read_service('posts') AS p
CROSS JOIN LATERAL UNNEST(p.tags) AS t
WHERE t.value = 'mysql';
```

### 11. Parser diagnostics examples
```sql
-- Malformed path after dot
SELECT address. FROM read_service('users');

-- Invalid array index syntax
SELECT tags[abc] FROM read_service('posts');

-- Malformed arrow path
SELECT education-> FROM read_service('users');
```
