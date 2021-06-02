package org.gs;

import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.inject.Inject;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.google.gson.Gson;

import io.vertx.mutiny.pgclient.PgPool;

@Path("posts")
public class PostResource {
    @Inject
    PgPool client;

    @POST
    public Uni<Response> create(String postString) throws JsonMappingException, JsonProcessingException {
        Gson gson = new Gson(); // Or use new GsonBuilder().create();
        Post post = gson.fromJson(postString, Post.class);
        return post.save(client)
                .onItem().transform(id -> URI.create("/post/" + id))
                .onItem().transform(uri -> Response.created(uri).build());
    }

    @GET
    public Multi<Post> get() {
        return getTagsByPostIDs(Post.findAll(client));
    }

    @GET
    @Path("{id}")
    public Uni<Response> getSingle(@PathParam("id") Long id) {
        return getTagsByPostID(Post.findById(client, id)).onItem().transform(post -> post != null ? Response.ok(post) : Response.status(Status.NOT_FOUND))
                .onItem().transform(ResponseBuilder::build);
    }

    @PUT
    @Path("{id}")
    public Uni<Response> update(@PathParam("id") Long id, Post post) {
        return post.update(client)
                .onItem().transform(updated -> updated ? Status.OK : Status.NOT_FOUND)
                .onItem().transform(status -> Response.status(status).build());
    }

    @DELETE
    @Path("{id}")
    public Uni<Response> delete(@PathParam("id") Long id) {
        return Post.delete(client, id)
                .onItem().transform(deleted -> deleted ? Status.OK : Status.NOT_FOUND)
                .onItem().transform(status -> Response.status(status).build());
    }

    private Multi<Post> getTagsByPostIDs(Multi<Post> posts) {
        ArrayList<Post> arr = new ArrayList<Post>();
        posts.subscribe().asIterable().forEach(p -> arr.add(getTag(p)));
        return Multi.createFrom().iterable(arr);
    }

    private Uni<Post> getTagsByPostID(Uni<Post> post) {
        return post.onItem().transform(p->getTag(p));
    }

    private Post getTag(Post post) {
        if (post == null) {
            return null;
        }
        Multi<Tag> multiTags = Post.findTagsByPostID(client, post.getPost_id());
        List<Tag> tags = new ArrayList<Tag>();
        multiTags.subscribe().asIterable().forEach(t->tags.add(t));
        post.setTags(tags);
        return post;
    }

    @PostConstruct
    void config() {
        initDB();
    }

    private void initDB(){
        client.query("DROP TABLE IF EXISTS posts").execute()
            .flatMap(m->client.query("CREATE TABLE posts (post_id serial PRIMARY KEY, title VARCHAR(255), content TEXT);").execute())
            .flatMap(m->client.query("INSERT INTO posts(title, content) VALUES ('Hidden Gem di Jakarta','lorem ipsum');").execute())
            .await()
            .indefinitely();
    }
}