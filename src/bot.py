"""
Discord bot that interacts with a database
"""
import discord
from .constants import (
    TOKEN
)
from .database import (
    create_connection,
    table_exists,
    table_operations,
    write_trans,
    delete_trans,
    update_trans,
    query_database,
    easy_query
)

intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)


@client.event
async def on_ready() -> None:
    """
    Runs when the bot is ready
    """
    print(f"Logged in as {client.user}")


@client.event
async def on_message(message: discord.Message) -> None:
    """
    Return a message for database commands
    """
    if message.content.startswith("db>"):
        conn = create_connection()

        if message.content.startswith("db>table"):
            msg = message.content.split(" ")[1:]

            if not msg:
                await message.channel.send("Missing required argument: operate_type & table_name")
                return

            elif len(msg) == 1:
                await message.channel.send("Missing required argument: table_name")
                return

            else:
                if msg[0] == "create":
                    if table_exists(conn, msg[1]):
                        await message.channel.send(
                            f"Table {msg[1]} already exists!"
                        )

                    else:
                        all_tables = table_operations(conn, msg[0], msg[1])
                        await message.channel.send(
                            f"Table {msg[1]} created! \n All tables: {all_tables}"
                        )
                elif msg[0] == "delete":
                    if table_exists(conn, msg[1]):
                        all_tables = table_operations(conn, msg[0], msg[1])
                        await message.channel.send(
                            f"Table {msg[1]} deleted! \n All tables: {all_tables}"
                        )
                    else:
                        await message.channel.send(
                            f"Table {msg[1]} does not exist!"
                        )

        elif message.content.startswith("db>write"):
            trans = message.content.split(" ")[1:]
            write_trans(conn, trans[0], trans[1], int(trans[2]), trans[3])
            await message.channel.send(
                f"Transaction {trans} written to database"
            )

        elif message.content.startswith("db>delete"):
            trans = message.content.split(" ")[1:]
            delete_trans(conn, trans[0], trans[1], trans[2])
            await message.channel.send(
                f"Transaction {trans} deleted from database"
            )

        elif message.content.startswith("db>update"):
            trans = message.content.split(" ")[1:]
            update_trans(conn, int(trans[0]), trans[1], trans[2], trans[3])
            await message.channel.send(
                f"Transaction {trans} updated with amount {trans[0]}"
            )

        elif message.content.startswith("db>query"):
            query = message.content.split(" ")[1:]
            query_type = query[0]
            fields = query[2] if len(query) > 2 else None
            filters = dict(zip(query[3::2], query[4::2])) if len(query) > 4 else None

            try:
                data = query_database(
                    conn, query_type, fields, filters
                )
                await message.channel.send(
                    f"Query type: {query_type}, data retrieved success: {data}")
            except ValueError:
                await message.channel.send("Missing required argument: query_type")

        elif message.content.startswith("db>ezquery"):
            data = easy_query(conn)
            await message.channel.send(f"Data retrieved: \n{data}")

if __name__ == "__main__":
    client.run(TOKEN, log_handler=None)
