"""Basic hello-world flow to verify Prefect worker connectivity."""

from prefect import flow, task


@task
def say_hello(name: str) -> str:
    greeting = f"Hello, {name}! Running on Prefect SPCS."
    print(greeting)
    return greeting


@flow(name="example-flow", log_prints=True)
def example_flow(name: str = "World"):
    """Simple flow that greets and returns a message."""
    result = say_hello(name)
    return result


if __name__ == "__main__":
    example_flow()
