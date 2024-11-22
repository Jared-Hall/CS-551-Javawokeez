import logging

#setup logger
def setupLogger(mode, level, logger, indentLevel, logfile="logfile.log"):
    logger.setLevel(level)
    fileHandler = logging.FileHandler(logfile, mode='w', encoding='utf-8')
    consoleHandler = logging.StreamHandler()
    indent = " "*indentLevel
    fmt = logging.Formatter(
        indent+"[{name}.{funcName}] {message}",
        style='{')

    if(mode):
        fileHandler.setFormatter(fmt)
        logger.addHandler(fileHandler)
    else:
        consoleHandler.setFormatter(fmt)
        logger.addHandler(consoleHandler)
    return logger
