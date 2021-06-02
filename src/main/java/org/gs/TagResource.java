package org.gs;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.inject.Inject;

import java.net.URI;

import javax.annotation.PostConstruct;

import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.pgclient.PgPool;

@Path("tags")
public class TagResource {
    @Inject
    PgPool client;

    @PostConstruct
    void config() {
        initDB();
    }

    @POST
    public Uni<Response> create(Tag tag)  {
        return tag.save(client)
                .onItem().transform(id -> URI.create("/tag/" + id))
                .onItem().transform(uri -> Response.created(uri).build());
    }

    @GET
    public Multi<Tag> get() {
        return getPosts(Tag.findAll(client));
    }

    @GET
    @Path("{id}")
    public Uni<Response> getSingle(@PathParam("id") Long id) {
        return Tag.findById(client, id)
                .onItem().transform(tag -> tag != null ? Response.ok(tag) : Response.status(Status.NOT_FOUND))
                .onItem().transform(ResponseBuilder::build);
    }

    @PUT
    public Uni<Response> update(Tag tag) {
        return tag.update(client)
                .onItem().transform(updated -> updated ? Status.OK : Status.NOT_FOUND)
                .onItem().transform(status -> Response.status(status).build());
    }

    @DELETE
    @Path("{id}")
    public Uni<Response> delete(@PathParam("id") Long id) {
        return Tag.delete(client, id)
                .onItem().transform(deleted -> deleted ? Status.OK : Status.NOT_FOUND)
                .onItem().transform(status -> Response.status(status).build());
    }

    private Multi<Tag> getPosts(Multi<Tag> tags) {
        tags.subscribe().asIterable().forEach(t -> getPost(t));
        return tags;
    }

    private Tag getPost(Tag tag) {
        tag.setPosts(Tag.findPostsByTagID(client, tag.getTag_id()));
        return tag;
    }

    private void initDB(){
        client.query("DROP TABLE IF EXISTS tags").execute()
            .flatMap(m->client.query("CREATE TABLE tags (tag_id serial PRIMARY KEY, label VARCHAR(255));").execute())
            .flatMap(m->client.query("INSERT INTO tags(label) VALUES ('fyp');").execute())
            .await()
            .indefinitely();

        client.query("DROP TABLE IF EXISTS posts_tags").execute()
            .flatMap(m->client.query("CREATE TABLE posts_tags (id serial PRIMARY KEY, post_id int REFERENCES posts (post_id) ON DELETE CASCADE ON UPDATE CASCADE, tag_id int REFERENCES tags (tag_id) ON DELETE CASCADE ON UPDATE SET NULL);").execute())
            .await()
            .indefinitely();

        client.query("INSERT INTO posts_tags(post_id, tag_id) VALUES (1, 1);").execute()
            .await()
            .indefinitely();
    }
}