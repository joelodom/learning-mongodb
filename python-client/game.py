HELP_STRING = """Available commands:

    help    This screen.
    exit    Exit the game.
"""

def read():
    return input("> ")

def evaluate(command):
    if command == "exit":
        exit(0)
    elif command == "help":
        return HELP_STRING
    
    return "I don't know what to do with that."

while True:
    command = read()
    response = evaluate(command)
    print(response)
    print()
