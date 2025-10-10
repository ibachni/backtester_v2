import asyncio


# coroutine function
async def main_1():
    print("start of main coroutine")


# main() -> coroutine object
# when called directly, error: coroutine "main" was never awaited
# print(main()) returns coroutine object

# run the main coroutine (need a coroutine object)
# asyncio.run(main_1())

"""
More complex example
"""

# The await keyword: can only be used inside a coroutine function


# Define a coroutine that simulates a time-consuming task
async def fetch_data_2(delay, id):
    print("Fetching data... id:", id)
    await asyncio.sleep(delay)
    print("Data fetched, id:", id)
    return {"data": "some data", "id": id}


async def main_2():
    print("Start of main coroutine")
    # task is coroutine object not yet executed (needs to be awaited)
    task_1 = fetch_data_2(2, 1)
    task_2 = fetch_data_2(2, 2)
    # await the fetch_data coroutine, pausing execution of main until fetch data completes
    # When awaiting = executing -> main pauses and waits until fetch_data has completed
    result_1 = await task_1
    print(f"received result :{result_1}")

    result_2 = await task_2
    print(f"received result :{result_2}")

    print("End of main coroutine")


# No real performance gain: duration: 4 seconds!


# run the main coroutine (need a coroutine object)
# asyncio.run(main_2())

"""
Tasks
As soon as a coroutine is sleep or waiting for something,
we can move on and start executing a new task!
"""


async def fetch_data_3(id, sleep_time):
    print("f coroutine {id} starting to detch data")
    await asyncio.sleep(sleep_time)
    return {"id": id, "data": f"sample data from coroutine{id}"}


async def main_3():
    # create tasks for running coroutines concurrently
    # creating tasks = scheduling a coroutine to run as quickly as possible
    task_1 = asyncio.create_task(fetch_data_3(1, 2))
    task_2 = asyncio.create_task(fetch_data_3(2, 3))
    task_3 = asyncio.create_task(fetch_data_3(3, 1))

    result_1 = await task_1
    result_2 = await task_2
    result_3 = await task_3

    print(result_1, result_2, result_3)


# asyncio.run(main())

"""
Gather
"""


async def fetch_data(id, sleep_time):
    print("f coroutine {id} starting to detch data")
    await asyncio.sleep(sleep_time)
    return {"id": id, "data": f"sample data from coroutine{id}"}


async def main():
    # run multiples coroutines concurrently
    # results collected in a list
    results = await asyncio.gather(fetch_data(1, 2), fetch_data(2, 1), fetch_data(3, 3))
    # not so good at error handling and does not stop other coroutine if one of them fails

    for result in results:
        print(f"Received result: {result}")


# asyncio.run(main())


"""
Task Group Function
Prefered way to bundle tasks because provides built in error handling
"""


async def fetch_data_4(id, sleep_time):
    print("f coroutine {id} starting to fetch data")
    await asyncio.sleep(sleep_time)
    return {"id": id, "data": f"sample data from coroutine{id}"}


async def main_4():
    tasks = []
    # task group (tg) create: Create tasks
    # when tasks are created within a taskgroup, all tasks within the group have to finish
    # (concurrently) before moving onto the next part of the code
    async with asyncio.TaskGroup() as tg:
        for i, sleep_time in enumerate([2, 1, 3], start=1):
            task = tg.create_task(fetch_data_4(i, sleep_time))
            tasks.append(task)

    # task is a asyncio tasks (thus access to "result" function)
    results = [task.result() for task in tasks]
    print(type(tasks[0]))

    for result in results:
        print(f"Received result: {result}")


# asyncio.run(main_4())


"""
Futures:
utilized in lower level programming
Promise of a future result; a result is to come in the future
"""


async def set_future_result(future, value):
    await asyncio.sleep(2)
    future.set_result(value)
    print(f"Set the future's result to: {value}")


async def main_5():
    # create a future object:
    loop = asyncio.get_running_loop()  # create event loop
    future = loop.create_future()  # create own future

    # Schedule setting the future's result
    # create a new
    asyncio.create_task(set_future_result(future, "Future result is ready"))

    # wait the future's result
    # we didnt await the task, but we awaited the result
    result = await future
    print(f"Received the future's result: {result}")


# asyncio.run(main_5())


"""
Synchronization
"""
# a shared variable
# might time to modify / operation on shared_resource
# no two corouritines should work at the object at the same time
shared_resource = 0

# an asyncio lock
lock = asyncio.Lock()


async def modify_shared_resource():
    global shared_resource
    # acquire the lock (context manager): Is any other coroutine working on this?
    # if yes, wait until its finished, if not go into block of code below
    async with lock:
        # critical selection starts: everything here needs to finish before lock is released
        print(f"Resource before modification: {shared_resource}")
        shared_resource += 1
        await asyncio.sleep(1)  # Simulate an IO operation
        print(f"Resource after modification: {shared_resource}")


async def main_6():
    await asyncio.gather(*(modify_shared_resource() for _ in range(5)))


# Asyncio.run is an event loop (schedule), scheduling the tasks one after another
# asyncio.run(main_6())

"""
semaphore
Allows multiple coroutines to have access to the same object at the same time
"""


async def access_resource(semaphore, resource_id):
    async with semaphore:
        # simulating accessing a limited resource

        print(f"accessing resource: {resource_id}")
        await asyncio.sleep(1)  # Simulate an IO operation
        print(f"releasing resource: {resource_id}")


async def main_7():
    semaphore = asyncio.Semaphore(3)  # allow 2 concurrent accesses
    await asyncio.gather(*(access_resource(semaphore, i) for i in range(5)))


# Asyncio.run is an event loop (schedule), scheduling the tasks one after another
# asyncio.run(main_7())


"""
Event
Blog areas of code until flag is true
"""


async def waiter(event):
    # 1. we start here
    print("waiting for the event to be set")
    await event.wait()
    # 3. then we continue here
    print("event has been set, continuing executiom")


async def setter(event):
    # 2. we await until the event has been set
    await asyncio.sleep(2)  # simulate some work
    event.set()
    print("event has been set")


async def main_8():
    # create event
    event = asyncio.Event()
    await asyncio.gather(waiter(event), setter(event))


asyncio.run(main_8())
