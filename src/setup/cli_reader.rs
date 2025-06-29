 use clap::{command, Arg, ArgMatches};
 use crate::err::AppError;
 use std::ffi::OsString;
 
 pub struct CliPars {
    pub data_date: String,
    pub flags: Flags, 
 }
 
 #[derive(Debug, Clone, Copy)]
 pub struct Flags {
    pub process_all: bool,
    pub process_mdr_data: bool,
    pub process_iec_data: bool,
    pub code_data: bool,
    pub transfer_to_who: bool,
    pub overwrite_ctg: bool,
    pub test_run: bool,
 }
 
 pub fn fetch_valid_arguments(args: Vec<OsString>) -> Result<CliPars, AppError>
 { 
    let parse_result = parse_args(args)?;

    // Data Date parameter has a defult value of "", therefore safe to unwrap
  
    let data_date = parse_result.get_one::<String>("data_date").unwrap();

    // Flag values are false if not present, true if present.
 
    let a_flag = parse_result.get_flag("a_flag");
    let mut m_flag = parse_result.get_flag("m_flag");
    let mut e_flag = parse_result.get_flag("e_flag");
    let c_flag = parse_result.get_flag("c_flag");
    let t_flag = parse_result.get_flag("t_flag");
    let v_flag = parse_result.get_flag("v_flag");
    let z_flag = parse_result.get_flag("z_flag");

    if a_flag == true {
       m_flag = true;
       e_flag = true;
    }
     
    if !m_flag && !e_flag 
          && !c_flag && !t_flag && !v_flag 
    {
        m_flag = true;  // mdr data processing the default if no flag
    }
 
    let flags = Flags {
        process_all: a_flag,
        process_mdr_data: m_flag,
        process_iec_data: e_flag,
        code_data: c_flag,
        transfer_to_who: t_flag,
        overwrite_ctg: v_flag,
        test_run: z_flag,
    };
 
    Ok(CliPars {
        data_date: data_date.clone(),
        flags: flags,
    })
 
 }
 
 
 fn parse_args(args: Vec<OsString>) -> Result<ArgMatches, clap::Error> {
 
     command!()
         .about("Processes data from AACT CTG database to an mdr ad schema")
         .arg(
            Arg::new("data_date")
           .short('d')
           .long("date")
           .required(false)
           .help("A string with a date in ISO format that gives the date of the data")
           .default_value("")
         )
         .arg(
             Arg::new("a_flag")
            .short('a')
            .long("all")
            .required(false)
            .help("A flag signifying importing of all data")
            .action(clap::ArgAction::SetTrue)
         )
         .arg(
             Arg::new("m_flag")
            .short('m')
            .long("mdr-data")
            .required(false)
            .help("A flag signifying importing of traditional mdr data - excludes iec data")
            .action(clap::ArgAction::SetTrue)
         )
         .arg(
             Arg::new("e_flag")
            .short('e')
            .long("iec-data")
            .required(false)
            .help("A flag signifying importing of iec data")
            .action(clap::ArgAction::SetTrue)
         )
         .arg(
             Arg::new("c_flag")
            .short('c')
            .long("code-data")
            .required(false)
            .help("A flag indicating data should be coded as far as possible using contextual source data")
            .action(clap::ArgAction::SetTrue)
         )
         .arg(
             Arg::new("t_flag")
            .short('t')
            .long("transfer-data")
            .required(false)
            .help("A flag indicating a summary of the data should be transferred to the WHO database")
            .action(clap::ArgAction::SetTrue)
         )
         .arg(
             Arg::new("v_flag")
            .short('v')
            .long("overwrite-data")
            .required(false)
            .help("A flag indicating the data should overwrite the data in the CTG database")
            .action(clap::ArgAction::SetTrue)
         )
        .arg(
             Arg::new("z_flag")
             .short('z')
             .long("test")
             .required(false)
             .help("A flag signifying that this is part of an integration test run - suppresses logs")
             .action(clap::ArgAction::SetTrue)
        )
     .try_get_matches_from(args)
 
 }
 
 
 #[cfg(test)]
 mod tests {
     use super::*;
     
     // Ensure the parameters are being correctly extracted from the CLI arguments
 
     #[test]
     fn check_cli_no_explicit_params() {
         let target = "dummy target";
         let args : Vec<&str> = vec![target];
         let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
         let res = fetch_valid_arguments(test_args).unwrap();
         assert_eq!(res.flags.process_mdr_data, true);
         assert_eq!(res.flags.test_run, false);
     }
 
     #[test]
     fn check_cli_with_m_flag() {
         let target = "dummy target";
         let args : Vec<&str> = vec![target, "-m"];
         let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
 
         let res = fetch_valid_arguments(test_args).unwrap();
         assert_eq!(res.flags.process_mdr_data, true);
         assert_eq!(res.flags.process_iec_data, false);
         assert_eq!(res.flags.code_data, false);
         assert_eq!(res.flags.transfer_to_who, false);
         assert_eq!(res.flags.overwrite_ctg, false);   
         assert_eq!(res.flags.test_run, false);
     }
 
     #[test]
     fn check_cli_with_a_flag() {
         let target = "dummy target";
         let args : Vec<&str> = vec![target, "-a"];
         let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
   
         let res = fetch_valid_arguments(test_args).unwrap();
         assert_eq!(res.flags.process_mdr_data, true);
         assert_eq!(res.flags.process_iec_data, true);
         assert_eq!(res.flags.code_data, false);
         assert_eq!(res.flags.transfer_to_who, false);
         assert_eq!(res.flags.overwrite_ctg, false);   
         assert_eq!(res.flags.test_run, false);
     }
 
     #[test]
     fn check_cli_with_z_flags() {
         let target = "dummy target";
         let args : Vec<&str> = vec![target, "-z"];
         let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
 
         let res = fetch_valid_arguments(test_args).unwrap();
         assert_eq!(res.flags.process_mdr_data, true);
         assert_eq!(res.flags.process_iec_data, false);
         assert_eq!(res.flags.code_data, false);
         assert_eq!(res.flags.transfer_to_who, false);
         assert_eq!(res.flags.overwrite_ctg, false);       
         assert_eq!(res.flags.test_run, true);
     }
      
    
     #[test]
     fn check_cli_with_most_params_explicit() {
         let target = "dummy target";
         let args : Vec<&str> = vec![target, "-m", "-e", "-c", "-t", "-v"];
         let test_args = args.iter().map(|x| x.to_string().into()).collect::<Vec<OsString>>();
 
         let res = fetch_valid_arguments(test_args).unwrap();
         assert_eq!(res.flags.process_mdr_data, true);
         assert_eq!(res.flags.process_iec_data, true);
         assert_eq!(res.flags.code_data, true);
         assert_eq!(res.flags.transfer_to_who, true);
         assert_eq!(res.flags.overwrite_ctg, true);
         assert_eq!(res.flags.test_run, false);
     }
 
 }
 
 