# Calcite SQL extensions for streaming

## Introduction

Streams are collections to records that flow continuously, and forever.
Unlike tables, they are not typically stored on disk, but flow over the
network and are held for short periods of time in memory.

Streams complement tables because they represent what is happening in the
present and future of the enterprise whereas tables represent the past.
It is very common for a stream to be archived into a table.

Like tables, you often want to query streams in a high-level language
based on relational algebra, validated according to a schema, and optimized
to take advantage of available resources and algorithms.

Calcite's SQL is an extension to standard SQL, not another 'SQL-like' language.
Streaming SQL is easy to learn for anyone who knows regular SQL. The results
are predictable

