pub mod config_reader;
pub mod log_helper;
pub mod cli_reader;

use crate::err::AppError;
use sqlx::postgres::{PgPoolOptions, PgConnectOptions, PgPool};
use std::path::PathBuf;
use cli_reader::{CliPars, Flags};
use std::fs;
use std::time::Duration;
use sqlx::ConnectOptions;
use config_reader::Config;
use std::sync::OnceLock;
use chrono::NaiveDate;

pub struct InitParams {
    pub data_date: String,
    pub log_folder: PathBuf,
    pub flags: Flags,
}

pub static LOG_RUNNING: OnceLock<bool> = OnceLock::new();

pub fn get_params(cli_pars: CliPars, config_string: &String) -> Result<InitParams, AppError> {

    // Called from lib::run as the initial task of the program.
    // Returns a struct that contains the program's parameters.
      
    // Normal import and / or processing and / or outputting
    // If folder name also given in CL args the CL version takes precedence
    
    let config_file: Config = config_reader::populate_config_vars(&config_string)?; 

    // If data date given in CL args the CL version takes precedence, 
    // else use the config file. Whatever the source Data date must also 
    // be a valid date. If not end the program with error message.

    let mut data_date = cli_pars.data_date;
    if data_date == "".to_string() {
        data_date =  config_file.data_details.data_date;  
    }

    data_date = match NaiveDate::parse_from_str(&data_date, "%Y-%m-%d") {
        Ok(_) => data_date,
        Err(_) => "".to_string(),
    };

    if data_date == "" {   // Raise an AppError...required data is missing.
        return Result::Err(AppError::MissingProgramParameter("data_date".to_string()));
    }


    let log_folder = config_file.folders.log_folder_path;  
    if !folder_exists (&log_folder) { 
        fs::create_dir_all(&log_folder)?;
    }
   
    // For execution flags read from the environment variables
    
    Ok(InitParams {
        data_date,
        log_folder,
        flags: cli_pars.flags,
    })

}


fn folder_exists(folder_name: &PathBuf) -> bool {
    let xres = folder_name.try_exists();
    let res = match xres {
        Ok(true) => true,
        Ok(false) => false, 
        Err(_e) => false,           
    };
    res
}
        


pub async fn get_db_pool() -> Result<PgPool, AppError> {  

    // Establish DB name and thence the connection string
    // (done as two separate steps to allow for future development).
    // Use the string to set up a connection options object and change 
    // the time threshold for warnings. Set up a DB pool option and 
    // connect using the connection options object.

    let db_name = match config_reader::fetch_db_name() {
        Ok(n) => n,
        Err(e) => return Err(e),
    };

    let db_conn_string = config_reader::fetch_db_conn_string(&db_name)?;  
   
    let mut opts: PgConnectOptions = db_conn_string.parse()
                    .map_err(|e| AppError::DBPoolError("Problem with parsing conection string".to_string(), e))?;
    opts = opts.log_slow_statements(log::LevelFilter::Warn, Duration::from_secs(5));

    PgPoolOptions::new()
        .max_connections(5) 
        .connect_with(opts).await
        .map_err(|e| AppError::DBPoolError(format!("Problem with connecting to database {} and obtaining Pool", db_name), e))
}


pub fn establish_log(params: &InitParams) -> Result<(), AppError> {

    if !log_running() {  // can be called more than once in context of integration tests
        log_helper::setup_log(&params.log_folder)?;
        LOG_RUNNING.set(true).unwrap(); // should always work
        log_helper::log_startup_params(&params);
    }
    Ok(())
}

pub fn log_running() -> bool {
    match LOG_RUNNING.get() {
        Some(_) => true,
        None => false,
    }
}


// Tests
#[cfg(test)]

mod tests {
    use super::*;
    use std::ffi::OsString;

    #[test]
    fn check_config_with_no_params_read_correctly() {

        let config = r#"
 [data]
 data_date="2025-06-25"

 [folders]
 log_folder_path="/home/steve/Data/MDR/MDR_Logs/aact/"
 
 [database]
 db_host="localhost"
 db_user="user_name"
 db_password="password"
 db_port="5432"
 db_name="aact"
 who_db_name="who"
 cxt_db_name="cxt"
 cgt_db_name="cgt"

"#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.data_date, "2025-06-25");
        assert_eq!(res.log_folder, PathBuf::from("/home/steve/Data/MDR/MDR_Logs/aact/"));
        assert_eq!(res.flags.process_mdr_data, true);
        assert_eq!(res.flags.process_iec_data, false);
        assert_eq!(res.flags.code_data, false);
        assert_eq!(res.flags.transfer_to_who, false);
        assert_eq!(res.flags.overwrite_ctg, false);   
        assert_eq!(res.flags.test_run, false);
        assert_eq!(res.flags.process_mdr_data, true);
        assert_eq!(res.flags.test_run, false);

    }
   

    #[test]
    fn check_cli_date_overrides_config_date() {

         let config = r#"
 [data]
 data_date="2025-06-25"

 [folders]
 log_folder_path="/home/steve/Data/MDR/MDR_Logs/aact/"
 
 [database]
 db_host="localhost"
 db_user="user_name"
 db_password="password"
 db_port="5432"
 db_name="aact"
 who_db_name="who"
 cxt_db_name="cxt"
 cgt_db_name="cgt"

 "#;
 let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target", "-d", "2025-03-03"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.data_date, "2025-03-03");
        assert_eq!(res.log_folder, PathBuf::from("/home/steve/Data/MDR/MDR_Logs/aact/"));
        assert_eq!(res.flags.process_mdr_data, true);
        assert_eq!(res.flags.process_iec_data, false);


    }


    #[test]
    #[should_panic]
    fn check_no_date_panics() {

         let config = r#"
 [data]
 data_date=""

 [folders]
 log_folder_path="/home/steve/Data/MDR/MDR_Logs/aact/"
 
 [database]
 db_host="localhost"
 db_user="user_name"
 db_password="password"
 db_port="5432"
 db_name="aact"
 who_db_name="who"
 cxt_db_name="cxt"
 cgt_db_name="cgt"

 "#;
        let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target", "-m", "-e"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let _res = get_params(cli_pars, &config_string).unwrap();
    }



    #[test]
    fn check_a_cli_flag_read_correctly() {

         let config = r#"
 [data]
 data_date="2025-06-25"

 [folders]
 log_folder_path="/home/steve/Data/MDR/MDR_Logs/aact/"
 
 [database]
 db_host="localhost"
 db_user="user_name"
 db_password="password"
 db_port="5432"
 db_name="aact"
 who_db_name="who"
 cxt_db_name="cxt"
 cgt_db_name="cgt"

 "#;
 let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target", "-a", "-d", "2025-08-04"];
        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.data_date, "2025-08-04");
        assert_eq!(res.log_folder, PathBuf::from("/home/steve/Data/MDR/MDR_Logs/aact/"));
        assert_eq!(res.flags.process_mdr_data, true);
        assert_eq!(res.flags.process_iec_data, true);
        assert_eq!(res.flags.code_data, false);
        assert_eq!(res.flags.transfer_to_who, false);
        assert_eq!(res.flags.overwrite_ctg, false);   
        assert_eq!(res.flags.test_run, false);

    }


    #[test]
    fn check_cli_flags_read_correctly() {
    let config = r#"
 [data]
 data_date="2025-06-25"

 [folders]
 log_folder_path="/home/steve/Data/MDR/MDR_Logs/aact/"
 
 [database]
 db_host="localhost"
 db_user="user_name"
 db_password="password"
 db_port="5432"
 db_name="aact"
 who_db_name="who"
 cxt_db_name="cxt"
 cgt_db_name="cgt"

 "#;

 let config_string = config.to_string();
        config_reader::populate_config_vars(&config_string).unwrap();

        let args : Vec<&str> = vec!["dummy target", "-m", "-e", "-t", "-c", "-v"];


        let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
        let cli_pars = cli_reader::fetch_valid_arguments(test_args).unwrap();

        let res = get_params(cli_pars, &config_string).unwrap();

        assert_eq!(res.data_date, "2025-06-25");
        assert_eq!(res.log_folder, PathBuf::from("/home/steve/Data/MDR/MDR_Logs/aact/"));
        assert_eq!(res.flags.process_mdr_data, true);
        assert_eq!(res.flags.process_iec_data, true);
        assert_eq!(res.flags.code_data, true);
        assert_eq!(res.flags.transfer_to_who, true);
        assert_eq!(res.flags.overwrite_ctg, true);   
        assert_eq!(res.flags.test_run, false);
        
    }
    
}

