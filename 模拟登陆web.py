import asyncio
from pyppeteer import launch


async def main():
    browser = await launch(headless=False)
    page = await browser.newPage()
    url = 'https://www.baidu.com/'
    await page.goto(url)
    # content = await page.content()
    content = await page.evaluate('document.body.textContent', force_expr=True)
    print(content)
    input()
    await browser.close()


asyncio.get_event_loop().run_until_complete(main())
