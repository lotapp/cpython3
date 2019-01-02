__all__ = 'run',

from . import coroutines
from . import events
from . import tasks


def run(main, *, debug=False):
    """
    运行一个协程
    Run a coroutine.

    此函数运行传递的协同程序，负责管理asyncio事件循环并完成异步生成器
    This function runs the passed coroutine, taking care of
    managing the asyncio event loop and finalizing asynchronous
    generators.

    当另一个asyncio事件循环在同一个线程中运行时，无法调用此函数。
    This function cannot be called when another asyncio event loop is
    running in the same thread.

    如果debug为True，则事件循环将以调试模式运行。
    If debug is True, the event loop will be run in debug mode.

    此函数始终创建一个新的事件循环并在结束时将其关闭。
    它应该用作asyncio程序的主要入口点，理想情况下应该只调用一次。
    This function always creates a new event loop and closes it at the end.
    It should be used as a main entry point for asyncio programs, and should
    ideally only be called once.

    Example:
        async def main():
            await asyncio.sleep(1)
            print('hello')

        asyncio.run(main())
    """
    # 3.7开始推荐使用"asyncio.get_running_loop()"来获取loop
    # 以前是直接使用"asyncio.get_event_loop()"
    if events._get_running_loop() is not None:
        raise RuntimeError("无法从正在运行的事件循环中调用asyncio.run()")

    if not coroutines.iscoroutine(main):
        raise ValueError("{!r}应该是一个协程".format(main))

    # 创建一个新的事件循环
    loop = events.new_event_loop()
    try:
        events.set_event_loop(loop)  # 设置事件循环
        loop.set_debug(debug)  # 是否调试运行（默认否）
        return loop.run_until_complete(main)  # 等待运行
    finally:
        try:
            _cancel_all_tasks(loop)  # 取消其他任务
            loop.run_until_complete(loop.shutdown_asyncgens())
        finally:
            events.set_event_loop(None)
            loop.close()


def _cancel_all_tasks(loop):
    to_cancel = tasks.all_tasks(loop)
    if not to_cancel:
        return

    for task in to_cancel:
        task.cancel()

    # 批量执行取消任务
    loop.run_until_complete(
        tasks.gather(*to_cancel, loop=loop, return_exceptions=True))

    for task in to_cancel:
        if task.cancelled():
            continue
        if task.exception() is not None:
            loop.call_exception_handler({
                'message': 'asyncio.run()关闭期间未处理的异常',
                'exception': task.exception(),
                'task': task,
            })