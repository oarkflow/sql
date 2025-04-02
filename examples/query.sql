SELECT p.* FROM read_service('posts') as p
JOIN read_service('comments') as c ON p.id = c.postId;
