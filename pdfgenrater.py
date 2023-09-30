import asyncio
from pyppeteer import launch

async def save_pdf(url, output_path):
    browser = await launch(headless=True)
    page = await browser.newPage()
    await page.goto(url)
    await page.pdf({'path': output_path, 'format': 'A4'})
    await browser.close()

if __name__ == '__main__':
    url = 'http://127.0.0.1:5000'  
    output_path = 'C:\\Users\\ANSHI MITTAL\\OneDrive\\Desktop\\flask project\output.pdf' 

    loop = asyncio.get_event_loop()
    loop.run_until_complete(save_pdf(url, output_path))
