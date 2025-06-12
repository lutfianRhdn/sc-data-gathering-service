export const convertDateFormat = (dateString: Date) => {
  // Parse the date string
  const date = new Date(dateString);

  // Extract the year, month, and day
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0"); // getUTCMonth() returns 0-based month
  const day = String(date.getUTCDate()).padStart(2, "0");

  // Format the date into the desired format
  const formattedDate = `${year}-${month}-${day}`;

  return formattedDate;
};
