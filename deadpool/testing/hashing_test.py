import hashlib


# Function to create a hash from given variables
def create_hash(name, wiki_page, wiki_id, age):
    combined_string = f"{name}{wiki_page}{wiki_id}{age}"
    return hashlib.sha256(combined_string.encode()).hexdigest()


# Initial variables
name = "Tina Turner"
wiki_page = ""
wiki_id = None
age = 58

# Creating the first hash
hash1 = create_hash(name, wiki_page, wiki_id, age)

# Updated variables
age = 58

# Creating the second hash
hash2 = create_hash(name, wiki_page, wiki_id, age)

print(hash1)
print(hash2)
print(hash1 == hash2)
