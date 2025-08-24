import { Page } from "@playwright/test";
import chalk from "chalk";

export const inputKeywords = async (
  page: Page,
  { SEARCH_FROM_DATE, SEARCH_TO_DATE, SEARCH_KEYWORDS, MODIFIED_SEARCH_KEYWORDS }
) => {
  try{
  // wait until it shown: input[name="allOfTheseWords"]
  await page.waitForSelector('input[name="allOfTheseWords"]', {
    state: "visible",
    timeout: 0,
  });

  await page.click('input[name="allOfTheseWords"]');

  if (SEARCH_FROM_DATE) {
    MODIFIED_SEARCH_KEYWORDS += ` since:${SEARCH_FROM_DATE}`;
  }

  if (SEARCH_TO_DATE) {
    MODIFIED_SEARCH_KEYWORDS += ` until:${SEARCH_TO_DATE}`;
  }

  console.info(chalk.yellow(`\nFilling in keywords: ${MODIFIED_SEARCH_KEYWORDS}\n`));

  await page.fill('input[name="allOfTheseWords"]', MODIFIED_SEARCH_KEYWORDS);

  // Press Enter
    await page.press('input[name="allOfTheseWords"]', "Enter");
  } catch (error) {
    console.error(chalk.red(`Error while inputting keywords: ${error.message}`));
    return  new Error(`Failed to input keywords: ${error.message}`);
  }
  
};
