import  chalk from 'chalk';

export default function log(message: string, level: 'info' | 'error' |'success'| 'warn' = 'info'): void { 
  const timestamp = new Date().toISOString();
  switch (level) {
    case 'info':
      console.log(chalk.whiteBright.bgBlue`{bold [INFO]}`, chalk`{blue [${timestamp}] ${message}}`);
      break;
    case 'error':
      console.error(chalk.whiteBright.bgRed`{bold [ERROR]}`, chalk`{red [${timestamp}] ${message}}`);
      break;
    case 'warn':
      console.warn(chalk.whiteBright.bgYellow`{bold [WARN]}`, chalk`{yellow [${timestamp}] ${message}}`);
      break;
    case 'success':
      console.log(chalk.whiteBright.bgGreen`{bold [SUCCESS]}`, chalk`{green [${timestamp}] ${message}}`);
      break;
    default:
      console.log(chalk.whiteBright.bgBlue`{bold [INFO]}`, chalk`{blue [${timestamp}] ${message}}`);
      break;
  }
}