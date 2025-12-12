from feast import Entity

user = Entity(
    name="user",
    join_keys=["user_id"],
    description="Identifiant unique permettant de relier les features à un utilisateur."
)
