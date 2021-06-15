import logging

# Create a custom logger
def get_standard_logger(name, file_path = None, overwrite_file = False, verbose = True, stream = True):
    '''
    This is a customized logger for general use with limited functionality.
    
    Parameters
    ----------  
    name : string
        Name to identify the logger uniquely
        
    verbose : bool, optional, default True
        Whether to set logging level as DEBUG-True/WARNING-FALSE
    
    file_path : string or None, optional, default None
        Outputs logs to given file_path file
        
    stream : bool, optional, default True
        Whether logs will be sent to outputstream, generally the screen
        
    Returns
    -------
    logger : A logger object with desired attributes
    
    Example
    -------
    import os
    current_path = os.path.dirname(os.path.abspath(__file__))
    logger_file_path = os.path.join(current_path, "log1.log")
    
    logger = get_standard_logger(__name__, file_path=logger_file_path, stream = True)
    
    logger.info("All Quality modules Loaded and Ready")
    
    logger.info("Value of X is : {}".format(X)) // cannot use 2 statements like print(S1, S2), logger.info(S1, S2) not allowed
    
    '''
    
    logger = logging.getLogger(name)
    
    # Set Logging Level for logger object to allow any verbose configuration of the handlers
    logger.setLevel(logging.DEBUG)
    # Add handlers if not added for given name
    if logger.handlers == []:
        
        # Create stream handler with formatter
        c_handler = logging.StreamHandler()
#         c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        c_format = logging.Formatter('%(message)s')
        c_handler.setFormatter(c_format)
        if verbose:
            c_handler.setLevel(logging.DEBUG)
            print("level set")
        else:
            c_handler.setLevel(logging.WARNING)

        # Create file handler, it's formatter and to logger if needed    
        if file_path != None:
            f_handler = logging.FileHandler(file_path, mode='w' if overwrite_file else 'a')
            f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            f_handler.setFormatter(f_format)
            if verbose:
                f_handler.setLevel(logging.DEBUG)
            else:
                f_handler.setLevel(logging.WARNING)
            
            logger.addHandler(f_handler)

        # Add stream handler to enable logger to output to screen
        if stream:
            logger.addHandler(c_handler)
            
    return logger