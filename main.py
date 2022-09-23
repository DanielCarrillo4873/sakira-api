"""
    Main application instance, main routes for api
"""

from fastapi import FastAPI
import mysql.connector
import request_models

db = mysql.connector.connect(user="admin", password="admin", host="localhost", database="sakila")

app = FastAPI()


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/api/v1/")
async def api_welcome():
    return {
        "title": "Sakila API",
        "api": "v1"
    }


@app.get("/api/v1/filmsByActor/{actor_id}")
async def films_by_actor(actor_id: str):
    cur = db.cursor(dictionary=True)
    sql = """SELECT film.title, film.rental_duration, film.release_year, film.rating, category.name as category
             FROM film_actor
             INNER JOIN film ON film_actor.film_id = film.film_id
             INNER JOIN film_category ON film.film_id = film_category.film_id
             INNER JOIN category ON film_category.category_id = category.category_id
             WHERE actor_id = %s ORDER BY actor_id; """
    cur.execute(sql, (actor_id,))
    results = cur.fetchall()
    cur.close()
    return {
        "results": results
    }


@app.get("/api/v1/totalCopiesByStore")
async def film_copies_by_store():
    cur = db.cursor(dictionary=True, buffered=True)
    sql = """   SELECT address.address AS store_address, film.title, COUNT(film.title) AS copies
                FROM store
                INNER JOIN address ON store.address_id = address.address_id
                INNER JOIN inventory ON store.store_id = inventory.store_id
                INNER JOIN film ON inventory.film_id = film.film_id
                GROUP BY address.address, film.title
                ORDER BY film.title;"""
    cur.execute(sql)
    cur.close()
    return {
        "results": cur.fetchall()
    }


@app.get("/api/v1/history/{client_id}")
async def client_history(client_id):
    cur = db.cursor(dictionary=True, buffered=True)
    sql = """   SELECT
                    film.title,
                    address.address,
                    rental.rental_date,
                    rental.return_date,
                    DATEDIFF(rental.return_date, rental.rental_date) AS loan_time,
                    payment.amount as cost,
                    payment.payment_date
                FROM rental
                INNER JOIN inventory ON rental.inventory_id = inventory.inventory_id
                INNER JOIN film ON inventory.film_id = film.film_id
                INNER JOIN store ON inventory.store_id = store.store_id
                INNER JOIN address ON store.address_id = address.address_id
                INNER JOIN payment ON rental.rental_id = payment.rental_id
                WHERE rental.customer_id=%s;"""
    cur.execute(sql, (client_id,))
    cur.close()
    return {
        "results": cur.fetchall()
    }


@app.get("/api/v1/subscriptionsByRegion")
async def subscriptions_by_region():
    cur = db.cursor(dictionary=True, buffered=True)
    sql = """   SELECT address.district, country.country, COUNT(customer_id) AS subsciptions
                FROM customer
                INNER JOIN address ON customer.address_id = address.address_id
                INNER JOIN city ON address.city_id = city.city_id
                INNER JOIN country ON city.country_id = country.country_id
                GROUP BY address.district, country.country
                ORDER BY address.district;"""
    cur.execute(sql)
    cur.close()
    return {
        "results": cur.fetchall()
    }


@app.post("/api/v1/actor")
async def create_actor(actor: request_models.Actor):
    cur = db.cursor(dictionary=True, buffered=True)
    cur.execute(
        "INSERT INTO actor (first_name, last_name) VALUE (%s, %s);",
        (actor.first_name, actor.last_name))
    cur.execute("SELECT * FROM actor WHERE actor_id = %s", (cur.lastrowid,))
    db.commit()
    cur.close()
    return cur.fetchone()


@app.delete("/api/v1/rental/{rental_id}")
async def delete_rental(rental_id):
    cur = db.cursor(dictionary=True, buffered=True)
    cur.execute("SELECT * FROM rental WHERE rental_id = %s", (rental_id,))
    rental = cur.fetchone()
    cur.execute("DELETE FROM rental WHERE rental_id = %s", (rental_id,))
    db.commit()
    cur.close()
    return rental
