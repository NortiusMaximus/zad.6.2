import sqlite3
from sqlite3 import Error
from os.path import join, dirname, abspath

db_file = join(dirname(dirname(abspath(__file__))), 'zad.6.2/teams_database.db')

def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by the db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
       conn = sqlite3.connect(db_file)
   except Error as e:
       print(e)

   return conn

def execute_sql(conn, sql):
   """ Execute sql
   :param conn: Connection object
   :param sql: a SQL script
   :return:
   """
   try:
       c = conn.cursor()
       c.execute(sql)
   except Error as e:
       print(e)

def add_team(conn, team):
   """
   Create a new team into the teams table
   :param conn:
   :param team:
   :return: team id
   """
   sql = '''INSERT INTO teams(name, founded, location)
             VALUES(?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, team)
   conn.commit()
   return cur.lastrowid

def add_player(conn, player):
   """
   Create a new player into the players table
   :param conn:
   :param player:
   :return: player id
   """
   sql = '''INSERT INTO players(team_id, name, surname, position, date_of_birth, height_in_cm)
             VALUES(?,?,?,?,?,?)'''
   cur = conn.cursor()
   cur.execute(sql, player)
   conn.commit()
   return cur.lastrowid

def select_all(conn, table):
   """
   Query all rows in the table
   :param conn: the Connection object
   :return:
   """
   cur = conn.cursor()
   cur.execute(f"SELECT * FROM {table}")
   rows = cur.fetchall()

   return rows

def select_where(conn, table, **query):
   """
   Query tasks from table with data from **query dict
   :param conn: the Connection object
   :param table: table name
   :param query: dict of attributes and values
   :return:
   """
   cur = conn.cursor()
   qs = []
   values = ()
   for k, v in query.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)
   cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
   rows = cur.fetchall()
   return rows

def update(conn, table, id, **kwargs):
   """
   update status, begin_date, and end date of a task
   :param conn:
   :param table: table name
   :param id: row id
   :return:
   """
   parameters = [f"{k} = ?" for k in kwargs]
   parameters = ", ".join(parameters)
   values = tuple(v for v in kwargs.values())
   values += (id, )

   sql = f''' UPDATE {table}
             SET {parameters}
             WHERE id = ?'''
   try:
       cur = conn.cursor()
       cur.execute(sql, values)
       conn.commit()
       print("OK, data updated.")
   except sqlite3.OperationalError as e:
       print(e)

def delete_where(conn, table, **kwargs):
   """
   Delete from table where attributes from
   :param conn:  Connection to the SQLite database
   :param table: table name
   :param kwargs: dict of attributes and values
   :return:
   """
   qs = []
   values = tuple()
   for k, v in kwargs.items():
       qs.append(f"{k}=?")
       values += (v,)
   q = " AND ".join(qs)

   sql = f'DELETE FROM {table} WHERE {q}'
   cur = conn.cursor()
   cur.execute(sql, values)
   conn.commit()
   print("Data deleted")

def delete_all(conn, table):
   """
   Delete all rows from table
   :param conn: Connection to the SQLite database
   :param table: table name
   :return:
   """
   sql = f'DELETE FROM {table}'
   cur = conn.cursor()
   cur.execute(sql)
   conn.commit()
   print("Data deleted")


if __name__ == "__main__":

   create_teams_sql = """
   -- teams table
   CREATE TABLE IF NOT EXISTS teams (
      id integer PRIMARY KEY,
      name text NOT NULL,
      founded integer NOT NULL,
      location text NOT NULL
   );
   """

   create_players_sql = """
   -- players table
   CREATE TABLE IF NOT EXISTS players (
      id integer PRIMARY KEY,
      team_id integer NOT NULL,
      name VARCHAR(250) NOT NULL,
      surname VARCHAR(250) NOT NULL,
      position VARCHAR(15) NOT NULL,
      date_of_birth text NOT NULL,
      height_in_cm integer NOT NULL,
      FOREIGN KEY (team_id) REFERENCES teams (id)
   );
   """

   conn = create_connection(db_file)
   if conn is not None:
        execute_sql(conn, create_teams_sql)
        execute_sql(conn, create_players_sql)
    
        team = ('Crusaders', 1996, 'Christchurch')
        team2 = ('Hurricanes', 1996, 'Wellington')

        team_id = add_team(conn, team)
        team_id2 = add_team(conn, team2)
        
        player = (
            team_id,
            "Richie",
            "Mo'unga",
            "First five-eighth",
            "25 May 1994",
            176
        )
        player2 = (
           team_id,
           'Scott',
           'Barrett',
           'Lock',
           '20 November 1993',
           197
        )
        player3 = (
           team_id2,
           'Ardie',
           'Savea',
           'Number 8',
           '14 October 1993',
           188
        )
        player4 = (
           team_id2,
           'Owen',
           'Franks',
           'Prop',
           '23 December 1987',
           188
        )
        player_id = add_player(conn, player)
        player_id2 = add_player(conn, player2)
        player_id3 = add_player(conn, player3)
        player_id4 = add_player(conn, player4)

        conn.commit()

        # wszystkie drużyny
        print(select_all(conn, "teams"))

        # wszyscy gracze o wzroście 188
        print(select_where(conn, "players", height_in_cm=188))

        update(conn, "players", 4, height_in_cm=187)

        delete_where(conn, "players", id=1)

        conn.close()