import os
import dotenv

dotenv.load_dotenv()


def main():
    manager_url = os.getenv("MANAGER_URL")
    print(f"Manager URL: {manager_url}")


if __name__ == "__main__":
    main()
