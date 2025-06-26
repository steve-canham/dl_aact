
pub mod setup;
pub mod mdr;
pub mod iec;
pub mod encode;
pub mod who;
pub mod ctg;
pub mod err;

use setup::cli_reader;
use err::AppError;
use std::ffi::OsString;
use std::path::PathBuf;
use std::fs;

pub async fn run(args: Vec<OsString>) -> Result<(), AppError> {

    let cli_pars: cli_reader::CliPars;
    cli_pars = cli_reader::fetch_valid_arguments(args)?;
    let _flags = cli_pars.flags;

    let config_file = PathBuf::from("./app_config.toml");
    let config_string: String = fs::read_to_string(&config_file)
                                .map_err(|e| AppError::IoReadErrorWithPath(e, config_file))?;
                              
    let params = setup::get_params(cli_pars, &config_string)?;
    setup::establish_log(&params)?;
    let pool = setup::get_db_pool().await?;
            
    if params.flags.process_mdr_data {
        mdr::do_mdr_import(&pool).await?;
    }
     
    if params.flags.process_iec_data {
        iec::do_iec_import(&pool).await?;
    }

    if params.flags.code_data {
        encode::do_data_encoding(&pool).await?;
    }

    if params.flags.transfer_to_who {
        who::do_who_transfer(&pool).await?;
    }

    if params.flags.overwrite_ctg {
        ctg::do_ctg_overwrite(&pool).await?;
    }


    Ok(())  
}
