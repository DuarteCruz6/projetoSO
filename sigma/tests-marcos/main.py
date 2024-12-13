import random
import string

# Change this to the number of test files you want to generate
NUMBER_OF_TESTS = 5


def generate_random_key(existing_keys):
    letters = string.ascii_lowercase
    digits = "0123456789"
    while True:
        key = random.choice(letters) + random.choice(digits)
        if key not in existing_keys:
            return key


def generate_random_value():
    return random.randint(0, 100)


def generate_command(existing_keys):
    if not existing_keys:
        # Possible commands: WRITE, BACKUP, WAIT, SHOW
        commands = ["WRITE", "BACKUP", "WAIT", "SHOW"]
        weights = [0.7, 0.1, 0.1, 0.1]
    else:
        # Possible commands: WRITE, READ, DELETE, BACKUP, WAIT, SHOW
        commands = ["WRITE", "READ", "DELETE", "BACKUP", "WAIT", "SHOW"]
        weights = [0.5, 0.2, 0.15, 0.05, 0.05, 0.05]

    command = random.choices(commands, weights)[0]

    if command == "WRITE":
        # Decide how many key-value pairs to write (1 to 5)
        n_pairs = random.randint(1, 5)
        pairs = []
        for _ in range(n_pairs):
            key = generate_random_key(existing_keys)
            value = generate_random_value()
            pairs.append(f"({key},{value})")
        pairs_str = "".join(pairs)  # No separator between pairs
        cmd_str = f"WRITE [{pairs_str}]"
        return cmd_str

    elif command == "READ":
        # Choose a key from existing_keys
        key = random.choice(sorted(existing_keys))
        cmd_str = f"READ [{key}]"
        return cmd_str

    elif command == "DELETE":
        # Decide how many keys to delete (1 to min(5, len(existing_keys)))
        n_keys = random.randint(1, min(5, len(existing_keys)))
        keys_to_delete = random.sample(sorted(existing_keys), n_keys)
        keys_str = ",".join(keys_to_delete)
        cmd_str = f"DELETE [{keys_str}]"
        return cmd_str

    elif command == "BACKUP":
        cmd_str = "BACKUP"
        return cmd_str

    elif command == "WAIT":
        wait_time = random.randint(1, 3) * 1000  # 1 to 3 seconds
        cmd_str = f"WAIT {wait_time}"
        return cmd_str

    elif command == "SHOW":
        cmd_str = "SHOW"
        return cmd_str


def update_existing_keys(existing_keys, command):
    # Update existing_keys based on the command
    if command.startswith("WRITE"):
        # Extract the pairs from the command
        pairs_str = command[len("WRITE [") : -1]  # Remove 'WRITE [' and ']'
        # The pairs are in the format (key1,value1)(key2,value2)
        pairs = []
        i = 0
        while i < len(pairs_str):
            if pairs_str[i] == "(":
                j = pairs_str.find(")", i)

                if j == -1:
                    break  # Should not happen

                pair = pairs_str[i + 1 : j]  # Exclude '(' and ')'
                pairs.append(pair)
                i = j + 1
            else:
                i += 1

        for pair in pairs:
            key_value = pair.split(",")
            if len(key_value) == 2:
                key = key_value[0]
                existing_keys.add(key)

    elif command.startswith("DELETE"):
        # Extract the keys from the command
        keys_str = command[len("DELETE [") : -1]
        keys = keys_str.split(",")
        for key in keys:
            if key in existing_keys:
                existing_keys.remove(key)
    # For READ, BACKUP, WAIT, SHOW, no change to existing_keys


def generate_job_file(filename):
    existing_keys = set()
    n_commands = random.randint(10, 100)
    commands = []

    for _ in range(n_commands):
        command = generate_command(existing_keys)
        commands.append(command)
        # Update existing_keys based on the command
        update_existing_keys(existing_keys, command)

    # Write commands to file
    with open(filename, "w") as f:
        for cmd in commands:
            f.write(cmd + "\n")


def generate_job_files(n_files):
    for i in range(n_files):
        filename = f"{i}.job"
        generate_job_file(filename)


if __name__ == "__main__":
    n_files = NUMBER_OF_TESTS  # You can set this to any number you like
    generate_job_files(n_files)
