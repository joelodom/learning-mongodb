"""
This is a toy program to have some fun while learning MongoDB.
Don't take it too seriously.

  -- Joel Odom
"""

from pymongo import MongoClient

HELP_STRING = """Available commands:

    help    This screen.
    exit    Exit the game.
    status  Show the status of the game.

    show
        universes         Shows all universes.
        rooms             Shows all rooms.
    
    create
        universe <name>   Creates a universe.
        room <name>       Creates a room. The name may include spaces.

    destroy
        universe <name>   Destroys a universe.
            
    teleport <name>    Takes you to an existing universe.
"""

MONGO_CLIENT = MongoClient('mongodb://localhost:27017/')
DB = None # This is the universe

RESERVED_DB_NAMES = ["admin", "local", "config"]
def is_reserved_universe(name):
    return name in RESERVED_DB_NAMES

def destroy_universe(db_name):
    global DB
    if is_reserved_universe(db_name):
        return "Naughty, naughty, naughty."
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
    return "I wish to destroy, but I know this thing not."

def teleport(s):
    global DB
    if len(s) < 2:
        return "Where shall I teleport?"
    db_name = s[1]
    if is_reserved_universe(db_name):
        return "Naughty, naughty, naughty."
    if db_name not in MONGO_CLIENT.list_database_names():
        return "Such a place does not exist. Yet."
    DB = MONGO_CLIENT[db_name]
    return f"You are now in universe {DB.name}."

def create_universe(db_name):
    global DB
    if is_reserved_universe(db_name):
        return "Naughty, naughty, naughty."
    if db_name in MONGO_CLIENT.list_database_names():
        return "Can one create that which already exists?"
    DB = MONGO_CLIENT[db_name]
    # It appears I have to add data to permanently create the database
    DB.properties.insert_one({"blank": ""})
    return f"Created a new universe, {db_name}. You are now there."

def list_universes():
    db_list = MONGO_CLIENT.list_database_names()
    return list(set(db_list) - set(RESERVED_DB_NAMES))

def list_rooms():
    rooms = []
    if DB is not None:
        query = {}
        projection = {
            "_id": 0,
            "name": 1
        }
        results = DB.rooms.find(query, projection)
        for doc in results:
            rooms.append(doc["name"])
    return rooms

def show(split_command):
    s = split_command
    if len(s) < 2:
        return "Show what?"
    if s[1] == "universes":
        return f"All known universes: {', '.join(list_universes())}"
    elif s[1] == "rooms":
        return f"All known rooms: {', '.join(list_rooms())}"
    return "I don't know how to show that."

def create_room(room_name):
    global DB
    if DB is None:
        return "You must be in a universe to create a room."
    DB.rooms.insert_one({"name":room_name})
    return f"Created a room called {room_name}."

def create(split_command):
    s = split_command
    if len(s) < 2:
        return "Create what?"
    if s[1] == "universe":
        if len(s) < 3:
            return "Remember to name your universe."
        universe_name = s[2]
        return create_universe(universe_name)
    elif s[1] == "room":
        if len(s) < 3:
            return "Remember to name your room. It may contain spaces."
        room_name = ' '.join(s[2:])
        return create_room(room_name)
    return "I don't know how to create that."

def read():
    return input("> ")

def status():
    universe = "You are not in any universe.";
    if DB is not None:
        universe = f"You are in universe {DB.name}."
    return f"""{universe}"""

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
    elif s[0] == "status":
        return status()
    
    return "I don't know what to do with that."

#
# TESTS
#
    
TEST_UNIVERSE_NAME = "test-universe-3141"
TEST_ROOM_NAME = "test room"

def cleanup_before_tests():
    if TEST_UNIVERSE_NAME in list_universes():
        evaluate(f"destroy universe {TEST_UNIVERSE_NAME}")

def test_monolothic_flow():
    assert(TEST_UNIVERSE_NAME not in list_universes())

    evaluate(f"create universe {TEST_UNIVERSE_NAME}")
    assert TEST_UNIVERSE_NAME in evaluate("show universes")

    evaluate(f"create room {TEST_ROOM_NAME}")
    assert TEST_ROOM_NAME in evaluate("show rooms")

    evaluate(f"destroy universe {TEST_UNIVERSE_NAME}")
    assert(TEST_UNIVERSE_NAME not in list_universes())

def run_all_tests():
    cleanup_before_tests()
    test_monolothic_flow()

run_all_tests()

#
# REPL
#

while True:
    command = read()
    response = evaluate(command)
    print(response)
    print()
