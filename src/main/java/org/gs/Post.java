package org.gs;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.smallrye.mutiny.Multi;
import io.vertx.mutiny.pgclient.PgPool;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.smallrye.mutiny.Uni;

public class Post {
    @JsonProperty("post_id")
    private long post_id;
    @JsonProperty("title")
    private String title;
    @JsonProperty("content")
    private String content;
    @JsonProperty("tags")
    private List<Tag> tags;

    public Post(){
        super();
    }

    public Post(Long post_id, String title, String content) {
        this.title = title;
        this.content = content;
        this.post_id = post_id;
    }
    

    public List<Tag> getTags() {
        return tags;
    }

    public void setTags(List<Tag> tags) {
        this.tags = tags;
    }

    public long getPost_id() {
        return post_id;
    }

    public void setPost_id(long post_id) {
        this.post_id = post_id;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public static Multi<Post> findAll(PgPool client) {
        return client.query("SELECT post_id, title, content FROM posts").execute()
        .onItem().transformToMulti(set -> Multi.createFrom().iterable(set))
        .onItem().transform(Post::from);
    }

    public static Multi<Tag> findTagsByPostID(PgPool client, long post_id) {
        return client.query("SELECT tags.tag_id, tags.label FROM posts_tags INNER JOIN tags ON posts_tags.tag_id = tags.tag_id WHERE posts_tags.post_id="+post_id).execute()
        .onItem().transformToMulti(set -> Multi.createFrom().iterable(set))
        .onItem().transform(Post::createTag);
    }

    public Uni<Long> save(PgPool client) {
        Uni<Long> id = client.preparedQuery("INSERT INTO posts (title, content) VALUES ($1, $2) RETURNING (post_id)").execute(Tuple.of(this.title, this.content))
                .onItem().transform(pgRowSet -> pgRowSet.iterator().next().getLong("post_id"));
        id.subscribe().with(pID -> {
            if (this.tags != null && this.tags.size() > 0) {
                for (Tag tag : this.tags) {
                    Tag.findById(client, tag.getTag_id()).subscribe().with(t->{
                        if (t==null){
                            Uni<Long> tagID = client.preparedQuery("INSERT INTO tags (label) VALUES ($1) RETURNING (tag_id)").execute(Tuple.of(tag.getLabel()))
                                                .onItem().transform(pgRowSet -> pgRowSet.iterator().next().getLong("tag_id"));
                            tagID.subscribe().with(tID -> savePostsTags(client, pID, tID));
                        } else {
                            savePostsTags(client, pID, t.getTag_id());
                        }
                    });
                }
            }
        });
        return id;
    }

    private void savePostsTags(PgPool client, Long postID, Long tagID) {
        Uni<Long> id = client.preparedQuery("INSERT INTO posts_tags (post_id, tag_id) VALUES ($1, $2) RETURNING (id)").execute(Tuple.of(postID, tagID)).onItem().transform(pgRowSet -> pgRowSet.iterator().next().getLong("id"));;
        id.subscribe().with(ids -> System.out.println("Success insert into posts_tags with post_id: "+postID+" and tag_id: "+tagID+" with id= "+ids));
    }

    public static Uni<Post> findById(PgPool client, Long id) {
        return client.preparedQuery("SELECT post_id, title, content FROM posts WHERE post_id = $1").execute(Tuple.of(id))
                .onItem().transform(RowSet::iterator)
                .onItem().transform(iterator -> iterator.hasNext() ? from(iterator.next()) : null);
    }

    public Uni<Boolean> update(PgPool client) {
        return client.preparedQuery("UPDATE posts SET title = $1, content = $2 WHERE post_id = $3").execute(Tuple.of(title, content, post_id))
                .onItem().transform(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    public static Uni<Boolean> delete(PgPool client, Long id) {
        return client.preparedQuery("DELETE FROM posts WHERE post_id = $1").execute(Tuple.of(id))
                .onItem().transform(pgRowSet -> pgRowSet.rowCount() == 1);
    }

    private static Post from(Row row) {
        return new Post(row.getLong("post_id"), row.getString("title"), row.getString("content"));
    }

    private static Tag createTag(Row row) {
        return new Tag(row.getLong("tag_id"), row.getString("label"));
    }
}