SELECT p.* FROM posts as p
JOIN comments as c ON p.id = c.postId LIMIT 1;
