import os
import sqlite3
import markdown
import time
import datetime

if __name__ == "__main__":
    from addons.handling.slugify import _slugify
    # For slugify see http://code.activestate.com/recipes/577257-slugify-make-a-string-usable-in-a-url-or-filename/
    test = True
else:
    test = False

# Need to add error handling for queries
## Need to add check for new titles in the titles file

## Quick class reference
## close() - closes the database connection
## createDb() - makes a new database
## initTitles() - gets the titles from the titles file for the self.titles variable
## getTitleFromFilename(filename) - self-explanatory
## updatePost(postId, filename, commit=False) - updates a single post in the database with
##          the id and filename provided. Only commits the info if commit=True
## addPost(filename, commit=False) - adds a new post to the database
## refresh() - updates existing posts in the database
## checkNewPosts() - adds posts from files that are not already in the database

class BlogDatabase():
    def __init__(self, path, postsDirectory={"titles":"posts/titles","visible":"posts/visible/"}):
        """Create the db connection with the given path"""
        self.postsDirectory = postsDirectory

        create = not os.path.isfile(path)
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.c = self.db.cursor()

        if create:
            self.createDb()

        ## initialise titles
        self.titles = {}
        self.initTitles()

    def close(self):
        """Close the cursor and connection"""
        self.c.close()
        self.db.close()

    def createDb(self):
        """Create a posts database should one not exist"""
        print "Creating new database."
        query = """CREATE TABLE posts (
        id    INTEGER NOT NULL,
        file  TEXT    NOT NULL,
        title TEXT,
        slug TEXT NOT NULL UNIQUE,
        body  TEXT    NOT NULL,
        oTime TEXT    NOT NULL,
        mTime  TEXT    NOT NULL,
        formattedDate TEXT NOT NULL,
        PRIMARY KEY (id)
        );"""
        self.c.execute(query)

        # Create first test post, can be removed later
        query = """INSERT INTO posts VALUES
        (0, 
        "posts/visible/1.markdown",
        "Test post #1",
        "test-post-1",
        "<h1>Testing</h1>
<p>This is the test post.</p>",
        "1347260529",
        "1347260529",
        "Magic Monday"
        );"""
        self.c.execute(query)
        self.db.commit()

    def initTitles(self):
        """Gets the titles from the titles file"""
        for line in open(self.postsDirectory["titles"], 'rU'):
            temp = line.strip().split("|")
            self.titles[self.postsDirectory['visible']+temp[0]] = temp[1]

    def getTitleFromFilename(self, filename):
        try:
            return self.titles[filename]
        except KeyError:
            self.initTitles()
            return self.titles[filename]

    def updatePost(self, postId, filename, commit=False):
        """Updates a single entry in the database"""
        # id, filename(full), title, body, oTime, mTime
        query = """UPDATE posts
        SET file = ?, title = ?, body = ?, mTime = ?
        WHERE id = ?;"""

        args = [filename, self.getTitleFromFilename(filename), \
                markdown.markdown(open(filename, 'r').read()), \
                os.stat(filename)[8], postId]

        self.c.execute(query, args)
        if commit:
            self.db.commit()

    def addPost(self, filename, commit=False):
        """Adds a post to the database"""
        #id, file, title, body, oTime, mTime
        query = """INSERT INTO posts(file, title, slug, body, oTime, mTime, formattedDate) VALUES
        (?, ?, ?, ?, ?, ?, ?);"""
        t = os.stat(filename)[8]
        tTitle = self.getTitleFromFilename(filename)
        args = [filename, tTitle, \
                _slugify(tTitle), \
                markdown.markdown(open(filename, 'rU').read()), \
                t, t, datetime.datetime.fromtimestamp(int(t)).strftime("%A, %b %d at %H:%M")]
        self.c.execute(query, args)
        if commit:
            self.db.commit()

    def refresh(self):
        """Update the timestamps of all existing posts in the database"""
        # Get the posts in db
        query = """SELECT id, file, mTime FROM posts;"""
        results = self.c.execute(query).fetchall()
        
        # Check the times in the database against the mtime on the files
        for row in results:
            if os.stat(row['file'])[8] > int(row['mTime']):
                self.updatePost(row['id'], row['file'])
                print "Updated:",row['file']
        self.db.commit()


    def checkNewPosts(self):
        """Check for new posts to add to the database"""
        # Get a list of filenames from database
        # Get a list of files
        # If file not in list of filenames then add it!
        query = """SELECT file FROM posts;"""

        # Find all files that are not in database
        filenames = set([x[0] for x in self.c.execute(query).fetchall()])
        files = set([self.postsDirectory['visible'] + x for x in os.listdir(self.postsDirectory['visible'])])
        newFiles = files.difference(filenames)

        if len(newFiles) == 0:
            print "There are no new files to add."
            return
        
        for f in newFiles:
            self.addPost(f)
            print "Added:", f
        self.db.commit()

    def getNewestPosts(self, limit=10):
        query = """SELECT title, body, formattedDate, slug FROM posts
        ORDER BY oTime desc
        LIMIT ?;"""
        return self.c.execute(query, [limit]).fetchall()

    def getNewestTitles(self, limit=10):
        query = """SELECT title, formattedDate, slug FROM posts
        ORDER BY oTime desc
        LIMIT ?;"""
        return self.c.execute(query, [limit]).fetchall()

    def getMultiplePosts(self, posts_ids):
        """Get all the posts in posts_ids"""
        query = """SELECT title, body, formattedDate, slug FROM posts
        WHERE id IN (?""" + ',?'*(len(posts_ids)-1) + """)
        ORDER BY oTime;"""
        return self.c.execute(query, posts_ids).fetchall()

    def listPosts(self):
        """List the posts in the database"""
        query = """SELECT id, title, file FROM posts ORDER BY id desc;"""
        results = self.c.execute(query)
        return results.fetchall()

    def getPostBySlug(self, post_slug):
        """Get the post with the given slug"""
        query = """SELECT title, body, formattedDate FROM posts
        WHERE slug = ?
        LIMIT 1;"""
        args = [post_slug]
        return self.c.execute(query, args).fetchone()

    def deletePost(self, postId, commit=False):
        """Delete a post"""
        query = """DELETE FROM posts WHERE id = ?;"""
        self.c.execute(query, [postId])
        if commit:
            self.db.commit()

    def deletePosts(self, postIds):
        """Delete multiple posts"""
        for pid in postIds:
            self.deletePost(pid)
        self.db.commit()


## Test stuff
if test:
    x = BlogDatabase("blog.sql")
