"""
This is a toy program to have some fun while learning MongoDB.
Don't take it too seriously.

  -- Joel Odom
"""

from pymongo import MongoClient

HELP_STRING = """Available commands:

    help    This screen.
    exit    Exit the game.

    show
        universes         Shows all universes.
    
    create
        universe <name>   Creates a universe.

    destroy
        universe <name>   Destroys a universe.
            
    teleport <name>    Takes you to an existing universe.
"""

MONGO_CLIENT = MongoClient('mongodb://localhost:27017/')
DB = None # This is the universe

def destroy_universe(db_name):
    global DB
    if db_name not in MONGO_CLIENT.list_database_names():
        return "Can one destroy that which does not exist?"
    if DB is not None:
        if DB.name == db_name:
            DB = None
    MONGO_CLIENT.drop_database(db_name)
    return f"You hear the wail of untold numbers of souls as {db_name} vanishes from existence."

def destroy(split_command):
    s = split_command
    if len(s) < 2:
        return "Destroy what?"
    if s[1] == "universe":
        if len(s) < 3:
            return "But what universe to destroy?"
        universe_name = s[2]
        return destroy_universe(universe_name)
    return "I wish to destroy, but I know this not."

def teleport(s):
    if len(s) < 2:
        return "Where shall I teleport?"
    db_name = s[1]
    if db_name not in MONGO_CLIENT.list_database_names():
        return "Such a place does not exist. Yet."
    DB = MONGO_CLIENT[db_name]
    return f"You are now in universe {DB.name}."

def create_universe(db_name):
    if db_name in MONGO_CLIENT.list_database_names():
        return "Can one create that which already exists?"
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
        create_universe(universe_name)
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
    elif s[0] == "destroy":
        return destroy(s)
    
    return "I don't know what to do with that."

while True:
    command = read()
    response = evaluate(command)
    print(response)
    print()

#
# TESTS
#
    
