"""
This is a toy program to have some fun while learning MongoDB.
Don't take it too seriously.

  -- Joel Odom
"""

from pymongo import MongoClient

HELP_STRING = """Available commands:

    help    This screen.
    exit    Exit the game.

    create
        universe < name >   Creates a universe.
    
    show
        universes           Shows all universes.
    
    teleport    < name>    Takes you to an existing universe.
"""

MONGO_CLIENT = MongoClient('mongodb://localhost:27017/')
DB = None # This is the universe

def teleport(s):
    if len(s) < 2:
        return "Where shall I teleport?"
    db_name = s[1]
    if db_name not in MONGO_CLIENT.list_database_names():
        return "Such a place does not exist. Yet."
    DB = MONGO_CLIENT[db_name]
    return f"You are now in universe {DB.name}."

def create_database(db_name):
    """
    Creates a new MongoDB database.

    Parameters:
    db_name (str): Name of the database to create.
    """
    # Create a new database or get an existing one
    DB = MONGO_CLIENT[db_name]

    # It appears I have to add data to really create the database
    DB.properties.insert_one({"blank": ""})

def show(split_command):
    "Shows some kind of information."

    s = split_command

    if len(s) < 2:
        return "Show what?"
    
    if s[1] == "universes":
        db_list = MONGO_CLIENT.list_database_names()
        return f"All known universes: {' '.join(db_list)}"
    
    return "I don't know how to show that."

def create(split_command):
    "Creates something."

    s = split_command

    if len(s) < 2:
        return "Create what?"

    if s[1] == "universe":
        if len(s) < 3:
            return "Remember to name your universe."
        universe_name = s[2]
        create_database(universe_name)
        return f"Created a new universe, {universe_name}. You are now there."
    
    return "I don't know how to create that."

def read():
    return input("> ")

def evaluate(command):
    s = command.split(" ")

    if s[0] == "exit":
        print("Good-bye, traveller.")
        print()
        exit(0)
    elif s[0] == "help":
        return HELP_STRING
    elif s[0] == "create":
        return create(s)
    elif s[0] == "show":
        return show(s)
    elif s[0] == "teleport":
        return teleport(s)
    
    return "I don't know what to do with that."

while True:
    command = read()
    response = evaluate(command)
    print(response)
    print()
