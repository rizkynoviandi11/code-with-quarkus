package org.gs;

import java.util.List;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;

public class Tag {
    @JsonProperty("tag_id")
    private long tag_id;
    @JsonProperty("label")
    private String label;
    @JsonProperty("posts")
    private List<Post> posts;

    public Tag(){
        super();
    }

    public Tag(Long tag_id, String label) {
        this.tag_id = tag_id;
        this.label = label;
    }

    public List<Post> getPosts() {
        return posts;
    }

    public void setPosts(Multi<Post> posts) {
        posts.collect().asList().subscribe().with(item -> this.posts = item, failure->failure.printStackTrace());
    }

    public long getTag_id() {
        return tag_id;
    }

    public void setTag_id(long tag_id) {
        this.tag_id = tag_id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public static Multi<Tag> findAll(PgPool client) {
        return client.query("SELECT tag_id, label FROM tags").execute()
        .onItem().transformToMulti(set -> Multi.createFrom().iterable(set))
        .onItem().transform(Tag::from);
    }

    public static Multi<Post> findPostsByTagID(PgPool client, long tag_id) {
        return client.query("SELECT posts.post_id, posts.title, posts.content FROM posts_tags INNER JOIN posts ON posts_tags.post_id = posts.post_id WHERE posts_tags.tag_id="+tag_id).execute()
        .onItem().transformToMulti(set -> Multi.createFrom().iterable(set))
        .onItem().transform(Tag::createPost);
    }

    public Uni<Long> save(PgPool client) {
        return client.preparedQuery("INSERT INTO tags (label) VALUES ($1) RETURNING (tag_id)").execute(Tuple.of(this.label))
                .onItem().transform(pgRowSet -> pgRowSet.iterator().next().getLong("tag_id"));
    }

    public static Uni<Tag> findById(PgPool client, Long id) {
        return client.preparedQuery("SELECT tag_id, label FROM tags WHERE tag_id = $1").execute(Tuple.of(id))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> iterator.hasNext() ? from(iterator.next()) : null);
    }

    public Uni<Boolean> update(PgPool client) {
        return client.preparedQuery("UPDATE tags SET label = $1 WHERE tag_id = $2").execute(Tuple.of(label, tag_id))
                .onItem().transform(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    public static Uni<Boolean> delete(PgPool client, Long id) {
        return client.preparedQuery("DELETE FROM tags WHERE tag_id = $1").execute(Tuple.of(id))
                .onItem().transform(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    private static Tag from(Row row) {
        return new Tag(row.getLong("tag_id"), row.getString("label"));
    }

    private static Post createPost(Row row) {
        return new Post(row.getLong("post_id"), row.getString("title"), row.getString("content"));
    }
}