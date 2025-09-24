use super::locs_utils::{replace_like_in_fac_proc, replace_regex_in_fac_proc, execute_sql,
                        replace_in_fac_proc, remove_leading_char_in_fac_proc, 
                        remove_trailing_char_in_fac_proc};

use sqlx::{Pool, Postgres};
use crate::AppError;
use log::info;


pub async fn do_section_header() -> Result<(), AppError> {  
    
    info!("------------------------------------------------------------------");
    info!(" Editing the fac_proc field for greater clarity and consistency");
    info!("------------------------------------------------------------------");
    info!("");
    Ok(())
}

pub async fn regularise_brackets(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_like_in_fac_proc( "[", "(", pool, true).await?;
    replace_like_in_fac_proc( "]", ")", pool, true).await?;
    replace_like_in_fac_proc( "{", "(", pool, true).await?;
    replace_like_in_fac_proc( "}", ")", pool, true).await?;
    info!("");
    Ok(())
}

pub async fn remove_enclosing_brackets(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    let sql = r#"update ad.locs
	set fac_proc = substring(fac_proc, 2, length(fac_proc) - 2)
	where fac_proc ~ '^\(.+\)$' and fac_proc !~ '^\(.+\(.+\)$'; "#;
    
    let res = execute_sql(sql, pool).await?;
    info!("{} records had enclosing brackets removed", res.rows_affected());
    info!("");
    Ok(())
}

pub async fn repair_non_ascii_1(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    info!(" Replacing non-utf with utf equivalents...");
    info!("");

    // Start by clearing the decks by removing these (apparently) redundant codes

    replace_regex_in_fac_proc( "â€�", "", pool, true).await?;
    replace_regex_in_fac_proc( "ï€£3", "", pool, true).await?;

    // Then do the following replacements, involving '�', often standing in
    // for several letters as well as different letters in different contexts
   
	replace_regex_in_fac_proc("Center �� Carmichael", "Center Carmichael", pool, true).await?;
	replace_regex_in_fac_proc("Zak��ad", "Zakład", pool, true).await?;
	replace_regex_in_fac_proc("H�pital", "Hôpital", pool, true).await?;
	replace_regex_in_fac_proc("Universit�Tsklinikum", "Universitätsklinikum", pool, true).await?;
	replace_regex_in_fac_proc("Investigaci�n Cl�nica", "Investigación Clínica", pool, true).await?;
	replace_regex_in_fac_proc("Centre L� B�rd", "Centre Léon Bérard", pool, true).await?;
	replace_regex_in_fac_proc("Universit�Tsmedizin", "Universitätsmedizin", pool, true).await?;
	replace_regex_in_fac_proc("C�te", "Côte", pool, true).await?;
	
    replace_regex_in_fac_proc("Besan�on", "Besançon", pool, true).await?;
	replace_regex_in_fac_proc("D�Hebron", "D’Hebron", pool, true).await?;
	replace_regex_in_fac_proc("H�al Saint-Antoine", "Hôpital Saint-Antoine", pool, true).await?;
	replace_regex_in_fac_proc("H�Pital Universitaire Piti�-Salp�Tri�Re", "Hôpital Universitaire Pitié-Salpêtrière", pool, true).await?;
	replace_regex_in_fac_proc("Pitie-Salpetri�re", "Pitié-Salpêtrière", pool, true).await?;
	replace_regex_in_fac_proc("Antoine Becl�re", "Antoine-Béclère", pool, true).await?;
	replace_regex_in_fac_proc("Servi�os", "Serviços", pool, true).await?;
	replace_regex_in_fac_proc("SE�Ra", "Señora", pool, true).await?;

	replace_regex_in_fac_proc("H�tel", "Hôtel", pool, true).await?;
	replace_regex_in_fac_proc("General Yag�", "General Yagüe", pool, true).await?;
	replace_regex_in_fac_proc("Gregorio Mara�on", "Gregorio Marañón", pool, true).await?;
	replace_regex_in_fac_proc("Est�ca do", "Estética do", pool, true).await?;
	replace_regex_in_fac_proc("\"Dermed�", "\"Dermed", pool, true).await?;
	replace_regex_in_fac_proc("Spitalul Jude�ean de Urgen�a dr. Constantin Opri�", "Spitalul Judeţean de Urgenţă Dr.Constantin Opriş", pool, true).await?;
	replace_regex_in_fac_proc("zaboliavania�", "zaboliavania", pool, true).await?;
    replace_regex_in_fac_proc("�L. E A. Ser�Gnoli�", "L. e A. Seragnoli", pool, true).await?;

    // Question marks in name demand slightly different tack
    replace_like_in_fac_proc("Szpitale Wojew�Dzkie W Gdyni Sp�?Ka Z Ograniczon? Odpowiedzialno?Ci?", "Szpitale Wojewódzkie w Gdyni Sp. z o.o", pool, true).await?;  

    info!("");
    Ok(())
}

pub async fn repair_non_ascii_2(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // A few strange ones need to be dealt with individually

    replace_regex_in_fac_proc( "Ã-rebro", "Örebro", pool, true).await?;
    replace_regex_in_fac_proc( " CittÃ", " Città", pool, true).await?;
    replace_regex_in_fac_proc( " UnitÃ", " Unità", pool, true).await?;
    replace_regex_in_fac_proc( "LaÃnnec", "Laënnec", pool, true).await?;
    replace_regex_in_fac_proc( "LaÃ\"nnec", "Laënnec", pool, true).await?;
    
    replace_regex_in_fac_proc( " CatalÃ", " Català", pool, true).await?;
    replace_regex_in_fac_proc( "Son LlÃ tzer", "Son Llàtzer", pool, true).await?;
    replace_regex_in_fac_proc( "Ã OK", "Á OK", pool, true).await?;
    replace_regex_in_fac_proc( "ParkinsonÃÂ¿s", "Parkinson’s", pool, true).await?;
    replace_regex_in_fac_proc( "OncologÃƒÂ-a", "Oncología", pool, true).await?;
    replace_regex_in_fac_proc( "LÃon BÃrard Centre RÃgional", "Léon Bérard Centre Régional", pool, true).await?;
    replace_regex_in_fac_proc( "GraubÃnden", "Graubünden", pool, true).await?;
    replace_regex_in_fac_proc( "Fundaã§Ã£O", "Fundação", pool, true).await?;
    replace_regex_in_fac_proc( "RenÃ ", "René ", pool, true).await?;
    replace_regex_in_fac_proc( "Presidentâ€™s ", "President’s ", pool, true).await?;
    replace_regex_in_fac_proc( "Oâ€™Neil", "O’Neil", pool, true).await?;
    replace_regex_in_fac_proc( "Marii SkÅ''odowskiej-Curie â€\" PaÅ\"stwowy Instytut", "Marii Skłodowskiej-Curie, Państwowy Instytut", pool, true).await?;
    replace_regex_in_fac_proc( "Researchâ€\" ", "Research, ", pool, true).await?;
    replace_regex_in_fac_proc( "the â€œHealth", "the Health", pool, true).await?;
    replace_regex_in_fac_proc( " â€œ", ", ", pool, true).await?;
    replace_regex_in_fac_proc( " â€\" ", ", ", pool, true).await?;

    info!("");

    // Then do these - a few for each of them
	
    replace_regex_in_fac_proc( "Ã©", "é", pool, true).await?;
    replace_regex_in_fac_proc( "Ã´", "ô", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¨", "è", pool, true).await?;
    replace_regex_in_fac_proc( "Ã-", "í", pool, true).await?;
    replace_regex_in_fac_proc( "Ã§", "ç", pool, true).await?;
    replace_regex_in_fac_proc( "Ã£", "ã", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¡", "á", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¶", "ö", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¤", "ä", pool, true).await?;
    replace_regex_in_fac_proc( "Ã³", "ó", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¼", "ü", pool, true).await?;
    replace_regex_in_fac_proc( "ã¼", "ü", pool, true).await?;
    replace_regex_in_fac_proc( "ÃŸ", "ß", pool, true).await?;
    replace_regex_in_fac_proc( "Ã±", "ñ", pool, true).await?;
    replace_regex_in_fac_proc( "Ãª", "ê", pool, true).await?;
    replace_regex_in_fac_proc( "Ã¢", "â", pool, true).await?;
    
    info!("");
    Ok(())
}
    
pub async fn repair_non_ascii_3(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_regex_in_fac_proc( "CARITï¿½ DI", "CARITÀ DI", pool, true).await?;
    replace_regex_in_fac_proc( "FRANï¿½OIS", "FRANÇOIS", pool, true).await?;
    replace_regex_in_fac_proc( "LIï¿½GE", "LIÈGE", pool, true).await?;
    replace_regex_in_fac_proc( "UNIVERSITï¿½TSMEDIZIN", "UNIVERSITÄTSMEDIZIN", pool, true).await?;
    replace_regex_in_fac_proc( "UNIVERSITï¿½TSKLINIKUM", "UNIVERSITÄTSKLINIKUM", pool, true).await?;
    replace_regex_in_fac_proc( "Hï¿½PITAL", "HÔPITAL", pool, true).await?;
    replace_regex_in_fac_proc( "UNIVERSITï¿½", "UNIVERSITÀ", pool, true).await?;
    replace_regex_in_fac_proc( "ST. MARYï¿½S HOSPITAL", "ST. MARY’S HOSPITAL", pool, true).await?;
    replace_regex_in_fac_proc( " ï¿½ ", " - ", pool, true).await?;

    replace_regex_in_fac_proc( "SZPITALE WOJEWï¿½DZKIE W GDYNI SPï¿½LKA Z OGRANICZONA ODPOWIEDZIALNOSCIA", "Szpitale Wojewódzkie w Gdyni Sp. z o.o.", pool, true).await?;


    replace_regex_in_fac_proc("Children¿s", "Children’s", pool, true).await?;
    replace_regex_in_fac_proc("D¿Hebron", "D’Hebron", pool, true).await?;
    replace_regex_in_fac_proc("Quinta D¿Or", "Quinta D’Or", pool, true).await?;
    replace_regex_in_fac_proc("Hospital ¿1", "Hospital No.1", pool, true).await?;
    replace_regex_in_fac_proc("6¿ City", "No.6 City", pool, true).await?;
    replace_regex_in_fac_proc("Hospital ¿ 442", "Hospital No.442", pool, true).await?;
    replace_regex_in_fac_proc("Institute¿Downriver", "Institute - Downriver", pool, true).await?; 
    replace_regex_in_fac_proc("Rafa¿", "Rafał", pool, true).await?;
    replace_regex_in_fac_proc(" ¿National Medical Research Oncology Centre named after N.N.", ", National Medical Research Center of Oncology named after N.N. Petrov", pool, true).await?;
    replace_regex_in_fac_proc("ZespAA GruAicy i ChorAb PA¿uc", "Zespół Gruźlicy i Chorób Płuc", pool, true).await?;
    replace_regex_in_fac_proc("¿KardioDent¿", "KardioDent", pool, true).await?;
    replace_regex_in_fac_proc("¿Sveti Naum¿", "Sveti Naum", pool, true).await?;
    replace_regex_in_fac_proc("¿Promedicus¿", "Promedicus", pool, true).await?;
    replace_regex_in_fac_proc("¿Attikon¿", "Attikon", pool, true).await?;
    replace_regex_in_fac_proc("¿Sf. Apostol Andrei¿", "Sf. Apostol Andrei", pool, true).await?;
    replace_regex_in_fac_proc("Charleroi ¿ Site Imtr", "Charleroi - Site IMTR", pool, true).await?;
    replace_regex_in_fac_proc("Sk¿odowskiej-Curie", "Skłodowska-Curie", pool, true).await?;
    replace_regex_in_fac_proc("Zak¿ad", "Zakład", pool, true).await?;
    replace_regex_in_fac_proc("Vitamed -Ga¿aj I", "Vitamed Gałaj i", pool, true).await?;
    replace_regex_in_fac_proc("Region Â¿Sverdlovsk", "Region, Sverdlovsk", pool, true).await?;
    replace_regex_in_fac_proc("Oddzia¿", "Oddział", pool, true).await?; 
    replace_regex_in_fac_proc(" ¿Region Clinical Hospital #3¿", ", Region Clinical Hospital #3", pool, true).await?;
    replace_regex_in_fac_proc(" ¿ ",  " - ", pool, true).await?; 

    info!("");
    Ok(())
}

pub async fn  remove_double_quotes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_like_in_fac_proc( "\"", "", pool, true).await?;
    replace_like_in_fac_proc( "&#34;", "", pool, true).await?;
    replace_like_in_fac_proc( "''''", "", pool, true).await?;
    
    info!("");
    Ok(())
}

pub async fn remove_single_quotes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_like_in_fac_proc( "&amp;", "&", pool, true).await?;
    replace_like_in_fac_proc( "&quot;", "", pool, true).await?;
    replace_like_in_fac_proc( "&#39;", "’", pool, true).await?;
    // The &amp; update has to be done twice
    replace_like_in_fac_proc( "&amp;", "&", pool, true).await?;
   
    info!("");
    Ok(())
}

pub async fn remove_double_commas(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_like_in_fac_proc( " ,,", " ", pool, true).await?;
    replace_like_in_fac_proc( ",, ", ", ", pool, true).await?;

    let sql = r#"update ad.locs
	set fac_proc = replace(fac_proc, ',,', '') where fac_proc like '%,,' or fac_proc like ',,%';"#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had ',,' removed from beginning or end of name", res); 

    let sql = r#"update ad.locs
	set fac_proc = replace(fac_proc, ',,', ', ') where fac_proc ~ '[A-za-z]+,,[A-za-z]+';"#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had ',,' replaced by ', ' when directly between text", res);
    
    info!("");        
    Ok(())
}

pub async fn process_apostrophes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Single apostrophe processing notmally converts to a right single quote (RSQ)
	// unless at the start of a line or after a space

	// The first group are a small cluster of Dutch names starting with 't or 's 
	// (single letter contractions of words for 'the' and 'of the'). 
    // The apostrophes are replaced by a right single quote.

    let sql = r#"update ad.locs 
	set fac_proc = replace(fac_proc, '''', '’') 
    where fac_proc ~ '^''[a-zA-Z] '; "#;
    let res1 = execute_sql(sql, pool).await?.rows_affected();
    
    let sql = r#"update ad.locs
	set fac_proc = replace(fac_proc, '''', '’') 
	where fac_proc ~ 's-Hertogenbosch' or fac_proc ~ 'Arnhem ''S Radiotherapeutisc';"#;
    let res2 = execute_sql(sql, pool).await?.rows_affected();

    info!("{} records had apostrophes before single letters replaced by RSQ", res1 + res2); 
    
    // a second group deals with a typo (especially common with some Chinese hospitals)

    let sql = r#"update ad.locs 
    set fac_proc = replace(fac_proc, ' ''s ', '’s ') 
	where fac_proc ~ ' ''s Hospit' or fac_proc ~ ' ''s Liberat'
	or fac_proc ~ ' ''s Research'; "#;
    let res1 = execute_sql(sql, pool).await?.rows_affected();
    
    // In the case below the 's seems to be an unnecessary addition, so it can be removed

    let sql = r#"update ad.locs 
    set fac_proc = replace(fac_proc, ' ''s ', ' ') 
	where fac_proc ~ 'Einstein ''s (IIEP)$'; "#;
    let res2 = execute_sql(sql, pool).await?.rows_affected();

    info!("{} records had apostrophes replaced / removed in typos", res1 + res2); 

    // in almost all other cases the presence of '% ''%' indicates
	// a pair of quotes around a name. They can both be removed.

    let sql = r#"update ad.locs 
    set fac_proc = replace(fac_proc, '''', '') 
	where fac_proc ~ ' '''; "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had apostrophes removed when found after a space", res); 

	// The remaining apostrophes are therefore single
	// right quotes acting as possessives or a contraction / ellision

    let sql = r#"update ad.locs 
    set fac_proc = replace(fac_proc, '''', '’') 
	where fac_proc ~ ''''; "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had apostrophes replaced by right single quotes", res); 
	
    info!("");
    Ok(())
}

pub async fn process_upper_ticks(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // The character is U+00b4. Seems to be used as an apostrophe in some 
    // settings (esp. Spanish records), and can be processed in a similar way

    // this odd one needs to be done first

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, 'd ´Hebron', 'd’Hebron')  where fac_proc ~ 'd ´Hebron'; "#;
    execute_sql(sql, pool).await?.rows_affected();

    // U+00b4 at the start of a line or after a space - indicates all can be removed from the line.

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '´', '')  where fac_proc ~ '^´'; "#;
    let res1 = execute_sql(sql, pool).await?.rows_affected();

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '´', '')  where fac_proc ~ ' ´';  "#;
    let res2 = execute_sql(sql, pool).await?.rows_affected();

    info!("{} records had '´' removed when found at beginning or after a space", res1 + res2); 

    // then the remainder - straightforward replacement (apart from one odd one that needs a )
 
    replace_regex_in_fac_proc( "´", "’", pool, true).await?;
    info!("");
    Ok(())
}

pub async fn remove_leading_trailing_odd_chars(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    remove_leading_char_in_fac_proc("-", pool, true).await?;
    remove_leading_char_in_fac_proc(".", pool, true).await?;
    remove_leading_char_in_fac_proc(",", pool, true).await?;
    remove_leading_char_in_fac_proc(":", pool, true).await?;
    remove_leading_char_in_fac_proc(";", pool, true).await?;
    remove_leading_char_in_fac_proc("&", pool, true).await?;
    remove_leading_char_in_fac_proc("•", pool, true).await?;
    remove_leading_char_in_fac_proc("_", pool, true).await?;

    remove_trailing_char_in_fac_proc("-", pool, true).await?;
    remove_trailing_char_in_fac_proc(".", pool, true).await?;
    remove_trailing_char_in_fac_proc(",", pool, true).await?;
    remove_trailing_char_in_fac_proc(":", pool, true).await?;
    remove_trailing_char_in_fac_proc(";", pool, true).await?;
    remove_trailing_char_in_fac_proc("&", pool, true).await?;
    remove_trailing_char_in_fac_proc("’", pool, true).await?;
    
    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '!', '1') where fac_proc ~ '^!'; "#;
    execute_sql(sql, pool).await?.rows_affected();

    let sql = r#"update ad.locs set fac_proc = trim(fac_proc) where fac_proc ~ '^ ' or fac_proc ~ ' $';  "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records with spaces at beginning or end trimmed", res); 

    info!("");
    Ok(())

}

pub async fn remove_underscores(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Need to do these particular situations first

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '_', '') where fac_proc ~ '[A-Za-z0-9#-]_ '; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had underscores removed when directly following text and before a space", res); 

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '_', '') where fac_proc ~ ' _[A-Za-z0-9#-]'; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had underscores removed when directly preceding text after a space", res); 

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '_', ' ') where fac_proc ~ '[A-Za-z0-9#-]_[A-Za-z0-9#-]'; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had underscores replaced by spaces when directly between text", res); 

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '_', '') where fac_proc ~ '_$'; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had underscores removed when at the end of the name", res); 

    // Strightforward replacements

	replace_regex_in_fac_proc(" _ ", " ", pool, true).await?; 
	replace_regex_in_fac_proc("._", ". ", pool, true).await?; 
	replace_regex_in_fac_proc("__", " ", pool, true).await?; 

    // Remainder 

	replace_regex_in_fac_proc("_", " ", pool, true).await?;  

    info!("");    
    Ok(())
}

pub async fn improve_comma_spacing(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // First deal with these few specific anomalies.

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ',.', ',') 
	where fac_proc like '%Policlinico A,. Gemelli%' or fac_proc like '%Genetics Unit,. Royal Manchester%'; "#; 
    execute_sql(sql, pool).await?.rows_affected();
	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ',.', '.,') where fac_proc like '%Co Ltd,. Haneda%' "#; 
	execute_sql(sql, pool).await?.rows_affected();

	// Then do the general insertion of a space after a comma next to text.
	    
    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ',', ', ') 
    where fac_proc ~ ',[A-Za-z0-9\(\)#]'; "#;  
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had spaces inserted after commas which were directly before text", res); 
  	
    // The process above adds many spurious spaces, along with existing anomalies of the kinds below

    replace_regex_in_fac_proc(",  ", ", ", pool, true).await?; 
	replace_regex_in_fac_proc(" , ", ", ", pool, true).await?; 
    
    info!("");  
    Ok(())
}

pub async fn improve_bracket_spacing(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // Right bracket

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ')', ') ') 
	where fac_proc ~ '\)[A-Za-z0-9#-]'; "#;
    let res = execute_sql(sql, pool).await?.rows_affected();   
	info!("{} records had spaces inserted after right bracket directly before text", res); 

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ')  ', ') ') 
	where fac_proc ~ '\)  '; "#;  
    let res = execute_sql(sql, pool).await?.rows_affected();
	info!("{} records had ')  ' replaced by ') '", res); 
	
	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ' ) ', ') ') 
	where fac_proc ~ ' \) '; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();
	info!("{} records had ' ) ' replaced by ') '", res); 
	
    // Left bracket

    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '(', ' (') 
	where fac_proc ~ '[A-Za-z0-9#.\)-]\(' and fac_proc !~ '\([a-z]\)'; "#;   
    let res = execute_sql(sql, pool).await?.rows_affected();
	info!("{} records had spaces inserted before left bracket directly after text", res); 

	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '  (', ' (') 
	where fac_proc ~ '  \('; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();  
    info!("{} records had '  (' replaced by ' ('", res); 
	
	let sql = r#"update ad.locs set fac_proc = replace(fac_proc, ' ( ', ' (') 
	where fac_proc ~ ' \( '; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();  
	info!("{} records had ' ( ' replaced by ' ('", res); 
	
    info!("");
    Ok(())
}

pub async fn remove_initial_numbering(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
   // remove_regexp_from_fac_proc(r"trim(replace(fac_proc, substring (fac_proc from '^[0-9]{1,2}\. '), ''))", r"'^[0-9]{1,2}\. '", "initial numbering removed", pool).await?;
   // remove_regexp_from_fac_proc(r"trim(replace(fac_proc, substring (fac_proc from '^[a-e]{1}\. '), ''))", r"'^[a-e]{1}\. '", "initial lettering removed", pool).await?;

    let sql = r#"update ad.locs c
	set fac_proc = trim(replace(fac_proc, substring (fac_proc from '^[0-9]{1,2}\. '), ''))
	where fac_proc ~ '^[0-9]{1,2}\. '; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();  
	info!("{} records had initial numbering removed", res); 

    let sql = r#"update ad.locs c
	set fac_proc = trim(replace(fac_proc, substring (fac_proc from '^[a-e]{1}\. '), ''))
    select * from ad.locs
    where fac_proc ~ '^[a-e]{1}\. '; "#; 
    let res = execute_sql(sql, pool).await?.rows_affected();  
	info!("{} records had initial lettering removed", res); 

    info!("");    
    Ok(())
}


pub async fn regularise_word_site(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // A few initial tidyings
    
    replace_in_fac_proc("Ssite", "Site", "r", true, "", pool).await?; 
    replace_in_fac_proc("site!", "Site", "r", true, "", pool).await?; 

    replace_in_fac_proc("SIte", "Site", r"fac_proc ~ ' SIte' or fac_proc ~ '^SIte' or fac_proc ~ '\(SIte'",  true, "", pool).await?; 
	
    replace_in_fac_proc("sites ", "Sites " , "fac_proc ~ '^sites '", true, "at the beginning of the name", pool).await?;  
    replace_in_fac_proc(" sites", " Sites", "fac_proc ~ ' sites$'",  true, "at the end of the name", pool).await?; 

    // Make number + s/Sites more consistent
	
    replace_in_fac_proc("sites", " Sites", "fac_proc ~ '[0-9]+sites'", true, "when immediately following a number", pool).await?;
    replace_in_fac_proc("Sites", " Sites", "fac_proc ~ '[0-9]+Sites'", true, "when immediately following a number", pool).await?;
    replace_in_fac_proc(" sites", " Sites", "fac_proc ~ '[0-9]+ sites'", true, "when following a number and a space", pool).await?;

    // Make SITE and SITES more consistent

    replace_in_fac_proc("ONCOSITE", "Oncosite", "r", true, "", pool).await?; 
    replace_in_fac_proc("INSITE", "Insite", "r", true, "", pool).await?; 

    replace_in_fac_proc(" SITES", " Sites", "fac_proc like '% SITES%'", true, "when at the end of the name", pool).await?;
    replace_in_fac_proc(" SITE", " Site", r#"fac_proc like '%SITE%' 
	and fac_proc not like '%VERSITE%' and fac_proc not like '%SITEM%'"#, true, "(with some exceptions)", pool).await?;
    
    // Make beginning and ending 'site' more consistent

    replace_in_fac_proc("site ", "Site ", "b", true, "at the beginning of the name", pool).await?; 
    replace_in_fac_proc(" site", " Site", "e", true, "at the end of the name", pool).await?; 

    replace_in_fac_proc("(site", "(Site", r"fac_proc ~ '\(site'", true, "when immediately preceded by '('", pool).await?; 
    replace_in_fac_proc(" site ", " Site ", "fac_proc ~ ' site [0-9]+$'", true, "when followed by a space and a number", pool).await?; 

    //Turn almost all remaining 'site' into 'Site' (exclude the 'further info' records)
    
    replace_in_fac_proc("-site", "- Site", "fac_proc ~ ' -site'", true, "", pool).await?; 
    replace_in_fac_proc("site-", "Site -", "fac_proc ~ ' site-'", true, "", pool).await?; 

    replace_in_fac_proc("site# ", "Site #", "fac_proc ~ 'site# [0-9]+'", true, "when followed directly by a number", pool).await?; 
    replace_in_fac_proc("site#", "Site #", "fac_proc ~ 'site#[0-9]+'", true, "when followed directly by a number", pool).await?; 

    replace_in_fac_proc(" site", " Site ", "fac_proc ~ ' site[0-9]+'", true, "when followed directly by a number", pool).await?; 
    replace_in_fac_proc(" site", " Site", r"fac_proc ~ ' site\)'", true, "when directly precedinmg ')'", pool).await?; 
 
    replace_in_fac_proc(" site,", " Site,", "r", true, "", pool).await?; 
    replace_in_fac_proc(" site:", " Site:", "r", true, "", pool).await?; 
    replace_in_fac_proc(" site;", " Site;", "r", true, "", pool).await?; 
        
    replace_in_fac_proc("-site", " - Site", r#"fac_proc ~ '-site'
	and fac_proc !~ 'multi-site' and fac_proc !~ 'on-site' and fac_proc !~ 'tri-site'"#, true, "(excluding genuinely hyphenated words)", pool).await?; 
    replace_in_fac_proc(" site ", " Site ", r#"fac_proc ~ ' site '
	and fac_proc !~ '^For ' and fac_proc !~ '^Contact '"#, true, "(excluding genuinely hyphenated words)", pool).await?; 

    // Try and regularise Site plus hashes and numbers
    
    replace_in_fac_proc("Site#", "Site #", "fac_proc ~ 'Site#[0-9]+'", true, "when directly followed by a number", pool).await?; 
    replace_in_fac_proc("Site# ", "Site #", "fac_proc ~ 'Site# [0-9]+'", true, "when followed by a spae and a number", pool).await?; 
    replace_in_fac_proc("Site # ", "Site #", "fac_proc ~ 'Site # [0-9]+'", true, "when directly followed by a number", pool).await?; 

    info!("");    

    /*
    
	--check!
	select * from ad.locs where fac_proc like '%site%'
	and fac_proc !~ '[A-Za-zé]site'
	and fac_proc !~ '^For '
	and fac_proc !~ '^Contact '
	and fac_proc !~ 'sites'
	order by fac_proc
    */
        
    Ok(())
}


pub async fn correct_place_names(_pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    /*update ad.locs set fac_proc = replace(fac_proc, 'germany', 'Germany') where fac_proc ~ 'germany'; 
	update ad.locs set fac_proc = replace(fac_proc, ' france', ' France') where fac_proc ~ ' france'; 
	update ad.locs set fac_proc = replace(fac_proc, ' russian', ' Russian') where fac_proc ~ ' russian'; 
	update ad.locs set fac_proc = replace(fac_proc, ' china', ' China') where fac_proc ~ ' china'; 
	update ad.locs set fac_proc = replace(fac_proc, 'turkey', 'Turkey') where fac_proc ~ 'turkey'; 
	update ad.locs set fac_proc = replace(fac_proc, 'israel', 'Israel') where fac_proc ~ 'israel'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'indiana', 'Indiana') where fac_proc ~ 'indiana'; 
	update ad.locs set fac_proc = replace(fac_proc, ' india', ' India') where fac_proc ~ ' india'; 
	update ad.locs set fac_proc = replace(fac_proc, 'pakistan', 'Pakistan') where fac_proc ~ 'pakistan'; 
	update ad.locs set fac_proc = replace(fac_proc, 'california', 'California') where fac_proc ~ 'california'; 
	update ad.locs set fac_proc = replace(fac_proc, 'baja ', 'Baja ') where fac_proc ~ 'baja '; 
	update ad.locs set fac_proc = replace(fac_proc, 'istanbul', 'Istanbul') where fac_proc ~ '^istanbul'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'cairo', 'Cairo') where fac_proc ~ 'cairo'; 
	update ad.locs set fac_proc = replace(fac_proc, 'cannes', 'Cannes') where fac_proc ~ 'cannes'; 
	update ad.locs set fac_proc = replace(fac_proc, 'asyut', 'Assiut') where fac_proc ~ 'asyut'; 
	update ad.locs set fac_proc = replace(fac_proc, 'assiut', 'Assiut') where fac_proc ~ 'assiut'; 
	update ad.locs set fac_proc = replace(fac_proc, 'assuit', 'Assiut') where fac_proc ~ 'assuit'; 
	update ad.locs set fac_proc = replace(fac_proc, 'aarhus', 'Aarhus') where fac_proc ~ 'aarhus'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'beijing', 'Beijing') where fac_proc ~ 'beijing'; 
	update ad.locs set fac_proc = replace(fac_proc, 'fayoum', 'Fayoum') where fac_proc ~ 'fayoum'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nantes', 'Nantes') where fac_proc ~ ' nantes' or fac_proc ~ '^nantes'; 
	update ad.locs set fac_proc = replace(fac_proc, 'korea', 'Korea') where fac_proc ~ 'korea';  
	update ad.locs set fac_proc = replace(fac_proc, 'marseille', 'Marseille') where fac_proc ~ 'marseille'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nice', 'Nice')where fac_proc ~ '^nice' or fac_proc ~ ' nice'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'radboud', 'Radboud') where fac_proc ~ 'radboud'; 
	update ad.locs set fac_proc = replace(fac_proc, 'rennes', 'Rennes') where fac_proc ~ 'rennes';  
	update ad.locs set fac_proc = replace(fac_proc, 'ruijin', 'Ruijin') where fac_proc ~ 'ruijin'; 
	update ad.locs set fac_proc = replace(fac_proc, 'shandong', 'Shandong')where fac_proc ~ '^shandong' or fac_proc ~ ' shandong';  
	update ad.locs set fac_proc = replace(fac_proc, 'shanghai', 'Shanghai')where fac_proc ~ '^shanghai' or fac_proc ~ ' shanghai';  
	update ad.locs set fac_proc = replace(fac_proc, 'sheffield', 'Sheffield') where fac_proc ~ 'sheffield'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'shengjing', 'Shengjing') where fac_proc ~ 'shengjing'; 
	update ad.locs set fac_proc = replace(fac_proc, 'shiraz', 'Shiraz') where fac_proc ~ 'shiraz'; 
	update ad.locs set fac_proc = replace(fac_proc, 'sydney', 'Sydney') where fac_proc ~ 'sydney'; 
	update ad.locs set fac_proc = replace(fac_proc, 'tel aviv', 'Tel Aviv') where fac_proc ~ 'tel aviv'; 
	update ad.locs set fac_proc = replace(fac_proc, 'zhongshan', 'Zhongshan') where fac_proc ~ 'zhongshan'; 
	update ad.locs set fac_proc = replace(fac_proc, 'zhujiang', 'Zhujiang') where fac_proc ~ 'zhujiang';  
	
	update ad.locs set fac_proc = replace(fac_proc, 'zhengzhou', 'Zhengzhou') where fac_proc ~ 'zhengzhou'; 
	update ad.locs set fac_proc = replace(fac_proc, 'brest', 'Brest')where fac_proc ~ '^brest' or fac_proc ~ ' brest';  
	update ad.locs set fac_proc = replace(fac_proc, 'caen', 'Caen') where fac_proc ~ '^caen' or fac_proc ~ ' caen'; 
	update ad.locs set fac_proc = replace(fac_proc, 'izmir', 'Izmir') where fac_proc ~ 'izmir'; 
	update ad.locs set fac_proc = replace(fac_proc, 'miami', 'Miami') where fac_proc ~ '^miami' or fac_proc ~ ' miami';
 */
        
    Ok(())
}


pub async fn correct_lower_case_beginnings(_pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    /* There are some anomalous lower case beginnings for some facility names
	-- On the other hands some companies / organisationsd genuinely have a lower case start
	-- and some are lower case because of missing first letters - therefore cannot do a blanket 
	-- capitalisation of the first letter

	-- Those below involve more than simple capitalisation 
	
	update ad.locs set fac_proc = 'The '||substring(fac_proc, 5) where fac_proc ~'^the '; 
	update ad.locs set fac_proc = replace(fac_proc, 'he ', 'The ') where fac_proc ~'^he ';  
	
	update ad.locs set fac_proc = replace(fac_proc, 'department', 'Department') where fac_proc ~'^department'; 
	update ad.locs set fac_proc = replace(fac_proc, 'dep ', 'Department ') where fac_proc ~'^dep '; 
	update ad.locs set fac_proc = replace(fac_proc, 'dep.', 'Department') where fac_proc ~'^dep.'; 
	update ad.locs set fac_proc = replace(fac_proc, 'dept', 'Department of') where fac_proc ~'^dept'; 
	update ad.locs set fac_proc = replace(fac_proc, 'dEP', 'Department of') where fac_proc ~'^dEP'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'faculty of medicine', 'Faculty of Medicine') where fac_proc ~'^faculty of medicine'; 
	update ad.locs set fac_proc = replace(fac_proc, 'faculty of Medicine', 'Faculty of Medicine') where fac_proc ~'^faculty of Medicine'; 
	update ad.locs set fac_proc = replace(fac_proc, 'faculty of dentistry', 'Faculty of Dentistry') where fac_proc ~'^faculty of dentistry'; 
	update ad.locs set fac_proc = replace(fac_proc, 'faculty of Dentistry', 'Faculty of Dentistry') where fac_proc ~'^faculty of Dentistry'; 
	update ad.locs set fac_proc = replace(fac_proc, 'faculty of ', 'Faculty of ') where fac_proc ~'^faculty of '; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'cLERMONT fERRAND', 'Clermont Ferrand') where fac_proc ~'cLERMONT fERRAND'; 
	update ad.locs set fac_proc = replace(fac_proc, 'fERNANDO bARATA', 'Fernando Barata') where fac_proc ~'fERNANDO bARATA';  

	update ad.locs set fac_proc = replace(fac_proc, 'at ', '') where fac_proc ~'^at '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ap-HM', 'AP-HM') where fac_proc ~'^ap-HM';  
	update ad.locs set fac_proc = replace(fac_proc, 'aQua', 'Aqua') where fac_proc ~'^aQua'; 
	update ad.locs set fac_proc = replace(fac_proc, 'azienda', 'Azienda') where fac_proc ~'^azienda '; 
	update ad.locs set fac_proc = replace(fac_proc, 'zienda', 'Azienda') where fac_proc ~'^zienda '; 
	update ad.locs set fac_proc = replace(fac_proc, 'az', 'AZ') where fac_proc ~'^az '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ain shams', 'Ain Shams') where fac_proc ~'^ain shams '; 
	update ad.locs set fac_proc = replace(fac_proc, 'am ', '') where fac_proc ~'^am '; 
	update ad.locs set fac_proc = replace(fac_proc, 'an Antonio', 'San Antonio ') where fac_proc ~'^an Antonio'; 
	update ad.locs set fac_proc = replace(fac_proc, 'a.o.', '') where fac_proc ~'^a\.o\.'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'bBston', 'Boston') where fac_proc ~'^bBston'; 
	update ad.locs set fac_proc = replace(fac_proc, 'b. ', '') where fac_proc ~'^b\. '; 
	update ad.locs set fac_proc = replace(fac_proc, 'c. ', '') where fac_proc ~'^c\. '; 
	update ad.locs set fac_proc = replace(fac_proc, 'c/o ', '') where fac_proc ~'^c/o '; 
	update ad.locs set fac_proc = replace(fac_proc, 'C/O ', '') where fac_proc ~'^C/O '; 
	update ad.locs set fac_proc = replace(fac_proc, 'cit ', 'CIT ') where fac_proc ~'^cit '; 
	update ad.locs set fac_proc = replace(fac_proc, 'coi ', 'COI ') where fac_proc ~'^coi ';

	update ad.locs set fac_proc = replace(fac_proc, 'de’ Montmorency', 'de’Montmorency') where fac_proc ~'^de’ Montmorency';
	update ad.locs set fac_proc = replace(fac_proc, 'd’Hebron', 'Vall d’Hebron') where fac_proc ~'^d’Hebron'; 
	update ad.locs set fac_proc = replace(fac_proc, 'all D’Hebron', 'Vall d’Hebron') where fac_proc ~'^all D’Hebron'; 
	update ad.locs set fac_proc = replace(fac_proc, 'dVeterans', 'Veterans') where fac_proc ~'^dVeterans'; 
	update ad.locs set fac_proc = replace(fac_proc, 'dba ', '') where fac_proc ~'^dba '; 
	update ad.locs set fac_proc = replace(fac_proc, 'd-b-a ', '') where fac_proc ~'^d-b-a ';
	update ad.locs set fac_proc = replace(fac_proc, 'ddC ', 'DDC') where fac_proc ~'^ddC '; 
	update ad.locs set fac_proc = replace(fac_proc, 'dGd ', 'DGD') where fac_proc ~'^dGd ';
	
	update ad.locs set fac_proc = replace(fac_proc, 'e Clatterbridge', 'Clatterbridge') where fac_proc ~'^e Clatterbridge';
	update ad.locs set fac_proc = replace(fac_proc, 'ezione', 'Sezione') where fac_proc ~'^ezione'; 
	update ad.locs set fac_proc = replace(fac_proc, 'eatson', 'Beatson') where fac_proc ~'^eatson'; 
	update ad.locs set fac_proc = replace(fac_proc, 'econd', 'Second') where fac_proc ~'^econd'; 
	update ad.locs set fac_proc = replace(fac_proc, 'from the o', 'O') where fac_proc ~'^from the o'; 
	update ad.locs set fac_proc = replace(fac_proc, 'f the ', 'The') where fac_proc ~'^f the '; 
	update ad.locs set fac_proc = replace(fac_proc, 'hildren', 'Children') where fac_proc ~'^hildren'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ialysis', 'Dialysis') where fac_proc ~'^ialysis'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ianjin', 'Tianjin') where fac_proc ~'^ianjin'; 
	update ad.locs set fac_proc = replace(fac_proc, 'i ASL', 'ASL') where fac_proc ~'^i ASL';
	update ad.locs set fac_proc = replace(fac_proc, 'iBS', 'ibs') where fac_proc ~'^iBS';

	update ad.locs set fac_proc = replace(fac_proc, 'i Can ', 'ICAN ') where fac_proc ~'^i Can'; 
	update ad.locs set fac_proc = replace(fac_proc, 'iCan ', 'ICAN ') where fac_proc ~'^iCan'; 
	update ad.locs set fac_proc = replace(fac_proc, 'I Can ', 'ICAN ') where fac_proc ~'^I Can '; 
	update ad.locs set fac_proc = replace(fac_proc, 'I CAN ', 'ICAN ') where fac_proc ~'^I CAN '; 
	update ad.locs set fac_proc = replace(fac_proc, 'inonuU', 'İnönü Üniversitesi') where fac_proc ~'^inonuU'; 
	update ad.locs set fac_proc = replace(fac_proc, 'icm ', 'ICM ') where fac_proc ~'^icm ';  --2
	update ad.locs set fac_proc = replace(fac_proc, 'ifo ', 'IFO ') where fac_proc ~'^ifo '; --1
	update ad.locs set fac_proc = replace(fac_proc, 'i. ', '') where fac_proc ~'^i\. '; --1
	update ad.locs set fac_proc = replace(fac_proc, 'ii. ', '') where fac_proc ~'^ii\. '; --1
	update ad.locs set fac_proc = replace(fac_proc, 'i Mei ', 'Chi Mei ') where fac_proc ~'^i Mei '; --1
	
	update ad.locs set fac_proc = replace(fac_proc, 'insaf', 'INSAF') where fac_proc ~'^insaf'; --1
	update ad.locs set fac_proc = replace(fac_proc, 'irccs', 'IRCCS') where fac_proc ~'^irccs'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ir Charles', 'Sir Charles') where fac_proc ~'^ir Charles'; 
	update ad.locs set fac_proc = replace(fac_proc, 'iverpool', 'Liverpool') where fac_proc ~'^iverpool'; --1
	update ad.locs set fac_proc = replace(fac_proc, 'ivision', 'Division') where fac_proc ~'^ivision'; 
	update ad.locs set fac_proc = replace(fac_proc, 'institut', 'Institut') where fac_proc ~'^institut'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lnstitut', 'Institut') where fac_proc ~'^lnstitut'; 
	update ad.locs set fac_proc = replace(fac_proc, 'istituto', 'Istituto') where fac_proc ~'^istituto'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lstituto', 'Istituto') where fac_proc ~'^lstituto'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nstituto', 'Instituto') where fac_proc ~'^nstituto';
	update ad.locs set fac_proc = replace(fac_proc, 'stitute', 'Institute') where fac_proc ~'^stitute'; 
	update ad.locs set fac_proc = replace(fac_proc, 'stituto', 'INstituto') where fac_proc ~'^stituto'; 

	update ad.locs set fac_proc = replace(fac_proc, 'epartment', 'Department') where fac_proc ~'^epartment'; 
	update ad.locs set fac_proc = replace(fac_proc, 'entre ', 'Centre ') where fac_proc ~'^entre '; 
	update ad.locs set fac_proc = replace(fac_proc, 'entrum Med', 'Centrum Med') where fac_proc ~'^entrum Med'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ervice', 'Service') where fac_proc ~'^ervice'; 
	update ad.locs set fac_proc = replace(fac_proc, 'est China', 'West China') where fac_proc ~'^est China';
	
	update ad.locs set fac_proc = replace(fac_proc, 'jmf', 'JMF') where fac_proc ~'^jmf '; 
	update ad.locs set fac_proc = replace(fac_proc, 'lcahn', 'Icahn') where fac_proc ~'^lcahn '; 
	update ad.locs set fac_proc = replace(fac_proc, 'linical', 'Clinical') where fac_proc ~'^linical '; 
	update ad.locs set fac_proc = replace(fac_proc, 'chu', 'CHU') where fac_proc ~'^chu '; 
	update ad.locs set fac_proc = replace(fac_proc, 'niversity', 'University') where fac_proc ~'^niversity'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nvestigative', 'Investigative') where fac_proc ~'^nvestigative';  
	update ad.locs set fac_proc = replace(fac_proc, 'llege', 'College') where fac_proc ~'^llege'; 
	update ad.locs set fac_proc = replace(fac_proc, 'll ', '') where fac_proc ~'^ll '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ospedale', 'Ospedale') where fac_proc ~'^ospedale ';
	update ad.locs set fac_proc = replace(fac_proc, 'ospdale', 'Ospedale') where fac_proc ~'^ospdale '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ospdali', 'Ospedale') where fac_proc ~'^ospdali '; 
	update ad.locs set fac_proc = replace(fac_proc, 'sanofi-aventi', 'Sanofi-Aventi') where fac_proc ~'^sanofi-aventi';  
	update ad.locs set fac_proc = replace(fac_proc, 'maha sadek', 'Maha Sadeks') where fac_proc ~'^maha sadek';  

	update ad.locs set fac_proc = replace(fac_proc, 'lnamdar', 'Inamdar') where fac_proc ~'^lnamdar'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lndraprastha', 'Indraprastha') where fac_proc ~'^lndraprastha'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lnje', 'Inje') where fac_proc ~'^lnje'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lnstytut', 'Instytut') where fac_proc ~'^lnstytut'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lntermed', 'Intermed') where fac_proc ~'^lntermed '; 
	update ad.locs set fac_proc = replace(fac_proc, 'lnvestigational', 'Ilnvestigational') where fac_proc ~'^lnvestigational'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lOP', 'IOP') where fac_proc ~'^lOP'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lRCCS', 'IRCCS') where fac_proc ~'^lRCCS'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lrmandade', 'Irmandade') where fac_proc ~'^lrmandade'; 
	update ad.locs set fac_proc = replace(fac_proc, 'lvanovo', 'Ivanovo') where fac_proc ~'^lvanovo'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ndiana', 'Indiana') where fac_proc ~'^ndiana'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ngShanghai', 'Shanghai') where fac_proc ~'^ngShanghai'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nineth', 'Ninth') where fac_proc ~'^nineth'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nited', 'United') where fac_proc ~'^nited'; 
	update ad.locs set fac_proc = replace(fac_proc, 'niversit', 'Universit') where fac_proc ~'^niversit'; 
	update ad.locs set fac_proc = replace(fac_proc, 'nstitut', 'Institut') where fac_proc ~'^nstitut'; 

	update ad.locs set fac_proc = replace(fac_proc, 'o ', '') where fac_proc ~'^o '; 
	update ad.locs set fac_proc = replace(fac_proc, 'of ', '') where fac_proc ~'^of '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ongji Hospital', 'Tongji Hospital') where fac_proc ~'^ongji Hospital'; 
	update ad.locs set fac_proc = replace(fac_proc, 'omplejo', 'Complejo') where fac_proc ~'^omplejo '; 
	update ad.locs set fac_proc = replace(fac_proc, 'ordan', 'Jordan') where fac_proc ~'^ordan '; 
	update ad.locs set fac_proc = replace(fac_proc, 'pitalul', 'Spitalul') where fac_proc ~'^pitalul '; 
	update ad.locs set fac_proc = replace(fac_proc, 'psrd', 'PSRD') where fac_proc ~'^psrd '; 
	update ad.locs set fac_proc = replace(fac_proc, 'qeii', 'QEII') where fac_proc ~'^qeii '; 
	update ad.locs set fac_proc = replace(fac_proc, 'r. Horst', 'Dr. Horst') where fac_proc ~'^r. Horst '; 
	update ad.locs set fac_proc = replace(fac_proc, 'rivat', 'Privat') where fac_proc ~'^rivat '; 
	update ad.locs set fac_proc = replace(fac_proc, 'rmandade', 'Irmandade') where fac_proc ~'^rmandade '; 
	update ad.locs set fac_proc = replace(fac_proc, 'sms', 'SMS') where fac_proc ~'^sms '; 

	update ad.locs set fac_proc = replace(fac_proc, 'spedale', 'Ospedale') where fac_proc ~'^spedale '; 
	update ad.locs set fac_proc = replace(fac_proc, 'spedali', 'Ospedali') where fac_proc ~'^spedali'; 
	update ad.locs set fac_proc = replace(fac_proc, 'stanbul', 'Istanbul') where fac_proc ~'^stanbul '; 
	update ad.locs set fac_proc = replace(fac_proc, 'st China', 'West China') where fac_proc ~'^st China '; 
	update ad.locs set fac_proc = replace(fac_proc, 'suAzio', 'SUAZIO') where fac_proc ~'^suAzio'; 
	update ad.locs set fac_proc = replace(fac_proc, 'td ', '') where fac_proc ~'^td '; 
	update ad.locs set fac_proc = replace(fac_proc, 'tlc', 'TLC') where fac_proc ~'^tlc '; 
	update ad.locs set fac_proc = replace(fac_proc, 'uc davis', 'UC Davis') where fac_proc ~'^uc davis '; 
	update ad.locs set fac_proc = replace(fac_proc, 'uijin', 'Ruijin') where fac_proc ~'^uijin '; 
	update ad.locs set fac_proc = replace(fac_proc, 'uz', 'UZ') where fac_proc ~'^uz '; 
	update ad.locs set fac_proc = replace(fac_proc, 'vzw', 'VZW') where fac_proc ~'^vzw '; 

	----------------------------------------------------------------------------------------------------------
	-- Provide temporary protection from blanket capitalisation by adding 'AAA' to the beginning of these genuineluy lower case terms

	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^aai'
	or fac_proc ~ '^aTyr' or fac_proc ~ '^analyze & realize' or fac_proc ~ '^bioskin'
	or fac_proc ~ '^cCare' or fac_proc ~ '^cyberDERM' or fac_proc ~ '^de '
	or fac_proc ~ '^dgd' or fac_proc ~ '^dermMedica' or fac_proc ~ '^doTERRA'
	or fac_proc ~ '^eMax' or fac_proc ~ '^eMKa' or fac_proc ~ '^emovis';
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^eStudy' or fac_proc ~ '^e-Study' or fac_proc ~ '^g\.SUND'
	or fac_proc ~ '^hVIVO' or fac_proc ~ '^i9 ' or fac_proc ~ '^iBiomed'
	or fac_proc ~ '^iCCM' or fac_proc ~ '^ifi' or fac_proc ~ '^ikfe'
	or fac_proc ~ '^hVIVO' or fac_proc ~ '^inVentiv' or fac_proc ~ '^iResearch';
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^nTouch' or fac_proc ~ '^bitop' or fac_proc ~ '^amO'
	or fac_proc ~ '^bioLytical' or fac_proc ~ '^daacro' or fac_proc ~ '^eResearch Technology'
	or fac_proc ~ '^eRT' or fac_proc ~ '^eThekwini' or fac_proc ~ '^eSSe'
	or fac_proc ~ '^de’Montmorency' or fac_proc ~ '^dell’' or fac_proc ~ '^cCARE'

	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^aCROnordic' or fac_proc ~ '^dTIP' or fac_proc ~ '^duPont'
	or fac_proc ~ '^eCare' or fac_proc ~ '^eCast' or fac_proc ~ '^eCommunity'
	or fac_proc ~ '^eps' or fac_proc ~ '^eStöd' or fac_proc ~ '^eStod'
	or fac_proc ~ '^estudy site' or fac_proc ~ '^eSupport' or fac_proc ~ '^framol-med'
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^go:h' or fac_proc ~ '^goMedus' or fac_proc ~* '^g\.Sund'
	or fac_proc ~ '^g\.tec' or fac_proc ~ '^http' or fac_proc ~ '^hyperCORE'
	or fac_proc ~ '^i3' or fac_proc ~ '^ibs' or fac_proc ~ '^icddr'
	or fac_proc ~ '^iD3' or fac_proc ~ '^iConquerMS' or fac_proc ~ '^ideSHi'
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^iDia' or fac_proc ~ '^ife' or fac_proc ~* '^iHealth'
	or fac_proc ~ '^iHope' or fac_proc ~ '^iKardio' or fac_proc ~ '^i Kokoro'
	or fac_proc ~ '^iMD' or fac_proc ~ '^iMedica' or fac_proc ~ '^iMED'
	or fac_proc ~ '^iMindU' or fac_proc ~ '^imland' or fac_proc ~ '^inContAlert'
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^iNeuro' or fac_proc ~ '^iOMEDICO' or fac_proc ~* '^ipb'
	or fac_proc ~ '^i-Research' or fac_proc ~ '^iSpecimen' or fac_proc ~ '^iSpine'
	or fac_proc ~ '^iThera' or fac_proc ~ '^iuvo' or fac_proc ~ '^ivWatch'
	or fac_proc ~ '^jMOG' or fac_proc ~ '^kbo' or fac_proc ~ '^kConFab'

	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^kfgn' or fac_proc ~ '^medamed' or fac_proc ~* '^medbo'
	or fac_proc ~ '^medicoKIT' or fac_proc ~ '^mediPlan' or fac_proc ~ '^medius'
	or fac_proc ~ '^mediX' or fac_proc ~ '^med.ring' or fac_proc ~ '^m&i'
	or fac_proc ~ '^mind ' or fac_proc ~ '^ms²' or fac_proc ~ '^myBETAapp'
	
	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^my mhealth' or fac_proc ~ '^nordBLICK' or fac_proc ~* '^nOvum'
	or fac_proc ~ '^physIQ' or fac_proc ~ '^pioh' or fac_proc ~ '^play2PREVENT'
	or fac_proc ~* '^proDerm' or fac_proc ~ '^pro mente ' or fac_proc ~ '^proSANUS'
	or fac_proc ~ '^pro scientia' or fac_proc ~ '^radprax' or fac_proc ~ '^rTMS'

	update ad.locs set fac_proc = 'ZZZ'||fac_proc
	where fac_proc ~ '^selectION' or fac_proc ~ '^suitX' or fac_proc ~* '^tecura'
	or fac_proc ~ '^terraXCube' or fac_proc ~ '^toSense' or fac_proc ~ '^uCT960+'
	or fac_proc ~* '^uEXPLORER' or fac_proc ~ '^uniQure' or fac_proc ~ '^xCures'
	or fac_proc ~ '^ze:ro' or fac_proc ~ '^zibp' 
		
	update ad.locs set fac_proc = upper(substring(fac_proc, 1, 1))||substring(fac_proc, 2)
	where fac_proc ~ '^[a-z]'
	
	-- recover 'protected' names
	update ad.locs set fac_proc = substring(fac_proc, 4) where fac_proc ~ '^ZZZ'


	--Check!
	select * from ad.locs where fac_proc ~ '^e'
	order by fac_proc
	select * from ad.locs where fac_proc ~ '^dep '
	order by fac_proc
	select * from ad.locs where fac_proc ~ '^[a-z]'
	order by fac_proc
	
*/
        
    Ok(())
}


pub async fn regularise_word_research(_pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    /*update ad.locs set fac_proc = replace(fac_proc, 'research', 'Research') where fac_proc like '%research%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'RESEARCH', 'Research') where fac_proc like '%RESEARCH%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'reseach', 'Research') where fac_proc like '%reseach%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseach', 'Research') where fac_proc like '%Reseach%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseacrh', 'Research') where fac_proc like '%Reseacrh%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseaerch', 'Research') where fac_proc like '%Reseaerch%';
	
	update ad.locs set fac_proc = replace(fac_proc, 'Researh', 'Research') where fac_proc like '%Researh%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseatch', 'Research') where fac_proc like '%Reseatch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reserach', 'Research') where fac_proc like '%Reserach%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseearch', 'Research') where fac_proc like '%Reseearch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reserch', 'Research') where fac_proc like '%Reserch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Resesarch', 'Research') where fac_proc like '%Resesarch%'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'Reasearch', 'Research') where fac_proc like '%Reasearch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reaseach', 'Research') where fac_proc like '%Reaseach%';
	update ad.locs set fac_proc = replace(fac_proc, 'Reaserach', 'Research') where fac_proc like '%Reaserach%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reearch', 'Research') where fac_proc like '%Reearch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Resarch', 'Research') where fac_proc like '%Resarch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reseaarch', 'Research') where fac_proc like '%Reseaarch%'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'Researcg', 'Research') where fac_proc like '%Researcg%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Researche', 'Research') where fac_proc like '%Researche%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Reserarch', 'Research') where fac_proc like '%Reserarch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Resezrch', 'Research') where fac_proc like '%Resezrch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Ressearch', 'Research') where fac_proc like '%Ressearch%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'RFesearch', 'Research') where fac_proc like '%RFesearch%';
	update ad.locs set fac_proc = replace(fac_proc, 'Rsearch', 'Research') where fac_proc like '%Rsearch%'; 

	 */
        
    Ok(())
}


pub async fn regularise_word_investigation(_pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    /* -- Investigational (Site)
	-- Start by ensuring capitalised versions of relevant words (N.B. not the spanish ones).
	-- The 'investigational site' entry type is used by pharma companies, and is therefore essentially English
	-- Also exclude the long 'For further informatrion' entries
	
	update ad.locs set fac_proc = replace(fac_proc, 'inv', 'Inv') 
	where (fac_proc like '% inves%' or fac_proc like 'inves%'
	or fac_proc like '%invers%' or fac_proc like '%inveti%' 
	or fac_proc like '%invsti%')
	and fac_proc ~* 'Site'
	and fac_proc !~ '^For '
	and fac_proc !~ 'investigac' and fac_proc !~ 'investigaç'
	and fac_proc !~ 'inform'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'ines', 'Ines') 
	where (fac_proc like '% inesti%' or fac_proc like 'inesti%')
	and fac_proc ~* 'Site'
	and fac_proc !~ '^For '
	and fac_proc !~ 'investigac' and fac_proc !~ 'investigaç'
	and fac_proc !~ 'inform';  

	--This small group needs to be added to the list additionally ('Site' missing in original)
	
	update ad.locs set fac_proc = replace(fac_proc, 'Trius investigator', 'Trius Investigator Site') 
	where fac_proc like '%Trius investigator%';
	
	--ensure all 'Site' rather than 'site' (still 25 appearing?) - may be sprious
	
	update ad.locs set fac_proc = replace(fac_proc, 'site', 'Site') 
	where (fac_proc like '% Inves%' or fac_proc like 'Inves%' 
	or fac_proc like '%Invers%' or fac_proc like '%Inveti%' 
	or fac_proc like '%Invsti%' or fac_proc like '%Inesti%')
	and fac_proc !~ 'Investigac' and fac_proc !~ 'Investigaç'
	and fac_proc ~ 'site'
	and fac_proc !~ '^For '
	and fac_proc !~ 'inform'; 

	update ad.locs set fac_proc = replace(fac_proc, 'Investigation Site', 'Investigational Site') where fac_proc like '%Investigation Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigator Site', 'Investigational Site') where fac_proc like '%Investigator Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigative Site', 'Investigational Site') where fac_proc like '%Investigative Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigate Site', 'Investigational Site') where fac_proc like '%Investigate Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investiational Site', 'Investigational Site') where fac_proc like '%Investiational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigtional Site', 'Investigational Site') where fac_proc like '%Investigtional Site%'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'Inverstigational Site', 'Investigational Site') where fac_proc like '%Inverstigational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invetigational Site', 'Investigational Site') where fac_proc like '%Invetigational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invesitgational Site', 'Investigational Site') where fac_proc like '%Invesitgational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invstigative Site', 'Investigational Site') where fac_proc like '%Invstigative Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investivative Site', 'Investigational Site') where fac_proc like '%Investivative Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigatice Site', 'Investigational Site') where fac_proc like '%Investigatice Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigating Site', 'Investigational Site') where fac_proc like '%Investigating Site%'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'Investgative Site', 'Investigational Site') where fac_proc like '%Investgative Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigationel Site', 'Investigational Site') where fac_proc like '%Investigationel Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Investigtive Site', 'Investigational Site') where fac_proc like '%Investigtive Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invesgational Site', 'Investigational Site') where fac_proc like '%Invesgational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invesigative', 'Investigational Site') where fac_proc like '%Invesigative Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Inestigational', 'Investigational Site') where fac_proc like '%Inestigational Site%'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Invesigational', 'Investigational Site') where fac_proc like '%Invesigational Site%'; 
*/
    
    Ok(())
}

pub async fn regularise_word_university(_pool: &Pool<Postgres>) -> Result<(), AppError> { 
/*
update ad.locs set fac_proc = replace(fac_proc, 'univerity', 'University') where fac_proc ~ 'univerity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univerity', 'University') where fac_proc ~ 'Univerity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'unversity', 'University') where fac_proc ~ 'unversity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Unversity', 'University') where fac_proc ~ 'Unversity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'univrsity', 'University') where fac_proc ~ 'univrsity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univrsity', 'University') where fac_proc ~ 'Univrsity'; 

    update ad.locs set fac_proc = replace(fac_proc, 'univeersity', 'University') where fac_proc ~ 'univeersity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univeersity', 'University') where fac_proc ~ 'Univeersity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'univerrsity', 'University') where fac_proc ~ 'univerrsity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univerrsity', 'University') where fac_proc ~ 'Univerrsity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'universsity', 'University') where fac_proc ~ 'universsity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Universsity', 'University') where fac_proc ~ 'universsity'; 

    update ad.locs set fac_proc = replace(fac_proc, 'univresity', 'University') where fac_proc ~ 'univresity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univresity', 'University') where fac_proc ~ 'Univresity'; 
    update ad.locs set fac_proc = replace(fac_proc, 'univeristy', 'University') where fac_proc ~ 'univeristy'; 
    update ad.locs set fac_proc = replace(fac_proc, 'Univeristy', 'University') where fac_proc ~ 'Univeristy'; 

    update ad.locs set fac_proc = replace(fac_proc, 'Duke Univ. Med. Ctr.', 'Duke University Medical Center') where fac_proc ~ 'Duke Univ. Med. Ctr.'; 
	update ad.locs set fac_proc = replace(fac_proc, 'univ. of', 'University of') where fac_proc ~ 'univ. of'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Univ. of', 'University of') where fac_proc ~ 'Univ. of'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Univ.', 'University') where fac_proc ~ 'Univ\.' and country ~ 'United States' ; 
*/
        
    Ok(())
}


pub async fn regularise_word_others(_pool: &Pool<Postgres>) -> Result<(), AppError> { 
/*
    -- others

	update ad.locs set fac_proc = replace(fac_proc, 'Repbulic', 'Republic') where fac_proc ~ 'Repbulic'; 
	update ad.locs set fac_proc = replace(fac_proc, 'recruting', 'recruiting') where fac_proc ~ 'recruting'; 
	update ad.locs set fac_proc = replace(fac_proc, 'recuiting', 'recruiting') where fac_proc ~ 'recuiting'; 
	update ad.locs set fac_proc = replace(fac_proc, 'medicall', 'medical') where fac_proc ~ 'medicall'; 
	update ad.locs set fac_proc = replace(fac_proc, 'Medicall', 'Medical') where fac_proc ~ 'Medicall'; 
	
    hospita 
    hospitall
    hosptal

*/
        
    Ok(())
}


pub async fn remove_upper_case_institut(_pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    /* update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT BERGONIE', 'Institut Bergonie') where fac_proc ~ 'INSTITUT BERGONIE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT CURIE', 'Institut Curie') where fac_proc ~ 'INSTITUT CURIE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT DE CANCEROLOGIE DE L’OUEST', 'Institut de Cancerolgie de l’ouest') where fac_proc ~ 'INSTITUT DE CANCEROLOGIE DE L’OUEST'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT JULES BORDET', 'Institut Jules Bordet') where fac_proc ~ 'INSTITUT JULES BORDET'; 

    update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO CATALAN DE ONCOLOGÍA', 'Instituto Catalan de Oncolgía') where fac_proc ~ 'INSTITUTO CATALAN DE ONCOLOGÍA'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO CATALAN DE ONCOLOGIA- L’HOSPITALET DE LLOBREGAT', 'Instituto Catalan de Oncolgía - L’Hospitalet de Llobregat') 
	where fac_proc ~ 'INSTITUTO CATALAN DE ONCOLOGIA- L’HOSPITALET DE LLOBREGAT'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO DE TERAPÉUTICA EXPERIMENTAL Y CLÍNICA', 'Instituto Catalan de Terapéutica Experimental y Clinica') 
	where fac_proc ~ 'INSTITUTO DE TERAPÉUTICA EXPERIMENTAL Y CLÍNICA'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO NACIONAL DE CANCEROLOGIA', 'Instituto Nacional de Cancerologia') where fac_proc ~ 'INSTITUTO NACIONAL DE CANCEROLOGIA'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO NACIONAL DE ENFERMEDADES NEOPLASICAS', 'Instituto Nacional de Enfermedades Neoplasicas') 
	where fac_proc ~ 'INSTITUTO NACIONAL DE ENFERMEDADES NEOPLASICAS'; 
    update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT PAOLI CALMETTES', 'Instituto Paoli Calmettes') where fac_proc ~ 'INSTITUT PAOLI CALMETTES'; 
    update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT REGIONAL DU CANCER MONTPELLIER', 'Institut Regional du Cancer Montpellier') 
	where fac_proc ~ 'INSTITUT REGIONAL DU CANCER MONTPELLIER'; 
	update ad.locs set fac_proc = replace(fac_proc, 'THE KITASATO INSTITUTE', 'The Kitasato Institute') where fac_proc ~ 'THE KITASATO INSTITUTE'; 
	
    update ad.locs set fac_proc = replace(fac_proc, 'STRAZHESKO INSTITUTE OF CARDIOLOGY, MAS OF UKRAINE', 'Strazhesko Institute of Cardiology, MAS of Ukraine') 
	where fac_proc ~ 'STRAZHESKO INSTITUTE OF CARDIOLOGY, MAS OF UKRAINE'; 
    update ad.locs set fac_proc = replace(fac_proc, 'WESLEY', 'Wesley') where fac_proc ~ 'WESLEY'; 
    update ad.locs set fac_proc = replace(fac_proc, 'CHILDREN’S CANCER', 'Children’s Cancer') where fac_proc ~ 'CHILDREN’S CANCER'; 
	update ad.locs set fac_proc = replace(fac_proc, 'FORSCHUNGSINSTITUT', 'Forschungsinstitut') where fac_proc ~ 'FORSCHUNGSINSTITUT'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'INVESTIGATION INSTITUTE', 'Investigation Institute') where fac_proc ~ 'INVESTIGATION INSTITUTE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO DO CÂNCER DO ESTADO DE SÃO PAULO', 'Instituto do Câncer do estado de São Paulo') where fac_proc ~ 'INSTITUTO DO CÂNCER DO ESTADO DE SÃO PAULO'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CURIE', 'Curie') where fac_proc ~ 'CURIE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'PARIS', 'Paris') where fac_proc ~ 'PARIS'; 
	
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT PAOLI CALMETTE', 'Instituto Paoli Calmettes') where fac_proc ~ 'INSTITUT PAOLI CALMETTE'; 
    update ad.locs set fac_proc = replace(fac_proc, 'PORTUGUES DE ONCOLOGIA DE LISBOA FRANCISCO GENTIL', 'Portugues de Oncologia de Lisboa Francisco Gentil') 
	where fac_proc ~ 'PORTUGUES DE ONCOLOGIA DE LISBOA FRANCISCO GENTIL'; 
	update ad.locs set fac_proc = replace(fac_proc, 'FOR SKIN ADVANCEMENT', 'for Skin Advancement') where fac_proc ~ 'FOR SKIN ADVANCEMENT'; 
    update ad.locs set fac_proc = replace(fac_proc, 'BAYLOR SCOTT & WHITE', 'Baylor Scott & White') where fac_proc ~ 'BAYLOR SCOTT & WHITE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ETICA', 'Etica') where fac_proc ~ 'ETICA'; 

	update ad.locs set fac_proc = replace(fac_proc, 'KITASATO UNIVERSITY KITASATO INSTITUTE HOSPITAL', 'Kitasato University Kitasato Institute Hospital') 
	where fac_proc ~ 'KITASATO UNIVERSITY KITASATO INSTITUTE HOSPITAL'; 
	update ad.locs set fac_proc = replace(fac_proc, 'ALL INDIA INSTITUTE OF MEDICAL SCIENCES, NEW DELHI', 'All India Institute of Medical Sciences, New Dehli') 
	where fac_proc ~ 'ALL INDIA INSTITUTE OF MEDICAL SCIENCES, NEW DELHI'; 
    update ad.locs set fac_proc = replace(fac_proc, 'TRANSLATIONAL', 'Translational') where fac_proc ~ 'TRANSLATIONAL'; 
	update ad.locs set fac_proc = replace(fac_proc, 'FOR METABOLISM AND DIABETES', 'for Metabolism and Diabetes') where fac_proc ~ 'FOR METABOLISM AND DIABETES'; 
    update ad.locs set fac_proc = replace(fac_proc, 'HASSMAN', 'Hassman') where fac_proc ~ 'HASSMAN'; 
	
    update ad.locs set fac_proc = replace(fac_proc, 'GROUPE HOSPITALIER MUTUALISTE INSTITUT de CANCEROLOGIE DANIEL HOLLARD', 
	'Groupe Hospitalier Mutualiste Institut de Cancerologie Daniel Hollard') 
	where fac_proc ~ 'GROUPE HOSPITALIER MUTUALISTE INSTITUT de CANCEROLOGIE DANIEL HOLLARD'; 
	update ad.locs set fac_proc = replace(fac_proc, 'DE EDUCAÇÃO, PESQUISA E GESTÃO EM SAÚDE', 'de Educação, pesquisa e geatão em saúde') 
	where fac_proc ~ 'DE EDUCAÇÃO, PESQUISA E GESTÃO EM SAÚDE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO MEXICANO DEL SEGURO SOCIAL UNIDAD MEDICA DE ALTA ESPECIALIDAD', 
	'Instituto Mexicano del Seguro Social Unidad Medica de Alta Especialidad') 
	where fac_proc ~ 'INSTITUTO MEXICANO DEL SEGURO SOCIAL UNIDAD MEDICA DE ALTA ESPECIALIDAD'; 

	update ad.locs set fac_proc = replace(fac_proc, 'INSALL-SCOTT-KELLY', 'Insall-Scott-Kelly') where fac_proc ~ 'INSALL-SCOTT-KELLY'; 
    update ad.locs set fac_proc = replace(fac_proc, 'CANCEROLOGIE DE L’OUEST-St HERBLAIN', 'Cancerologie de l’ouest-St Herblain') where fac_proc ~ 'CANCEROLOGIE DE L’OUEST-St HERBLAIN'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CATALA DE LA SALUT', 'Catala de la Salut') where fac_proc ~ 'CATALA DE LA SALUTY'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CLAUDIUS REGAUD', 'Claudius Regaud') where fac_proc ~ 'CLAUDIUS REGAUD'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CANCEROLOGIE DE L’OUEST', 'Cancerologie de l’ouest') where fac_proc ~ 'CANCEROLOGIE DE L’OUEST'; 

	update ad.locs set fac_proc = replace(fac_proc, 'DE CANCEROLOGIE DE MONTPELLIER', 'de Cancerologie de Montpellier') where fac_proc ~ 'DE CANCEROLOGIE DE MONTPELLIER'; 
	update ad.locs set fac_proc = replace(fac_proc, 'DE CANCEROLOGIE STRASBOURG EUROPE', 'de Cancerologie Strasbourg Europe') where fac_proc ~ 'DE CANCEROLOGIE STRASBOURG EUROPE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'DE CANCEROLOGIE', 'de Cancerologie') where fac_proc ~ 'DE CANCEROLOGIE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'D’HEMATOLOGIE DE BASSE NORMANDIE', 'd’Hematologie de Basse Normande') where fac_proc ~ 'D’HEMATOLOGIE DE BASSE NORMANDIE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'GUSTAVE ROUSSY', 'Gustave Roussy') where fac_proc ~ 'GUSTAVE ROUSSY'; 

    update ad.locs set fac_proc = replace(fac_proc, 'DE INVESTIGACIONES METABOLICAS', 'de Investigaciones Metabolicas') where fac_proc ~ 'DE INVESTIGACIONES METABOLICAS'; 
    update ad.locs set fac_proc = replace(fac_proc, 'DO CORAÇÃO', 'do Coração') where fac_proc ~ 'DO CORAÇÃO'; 
	update ad.locs set fac_proc = replace(fac_proc, 'PORTUGUES DE ONCOLOGIA DE LISBOA FRANCISCO GENTIL', 'Portugues de Oncologia de Lisboa Francisco Gentil') 
	where fac_proc ~ 'PORTUGUES DE ONCOLOGIA DE LISBOA FRANCISCO GENTIL'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CANCER MONTPELLIER - ICM - VAL d’AURELLE', 'Cancer Montpellier - ICM - Val d’Aurelles') 
	where fac_proc ~ 'CANCER MONTPELLIER - ICM - VAL d’AURELLE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'CARDIOLOGY INSTITUTE', 'Cardiology Institute') where fac_proc ~ 'CARDIOLOGY INSTITUTE'; 
	
   	update ad.locs set fac_proc = replace(fac_proc, 'PARTNERS LLC DBA NEW ENGLAND INSTITUTE FOR CLINICAL', 'Partners LLC DBA New England Institute for clinical') 
	where fac_proc ~ 'PARTNERS LLC DBA NEW ENGLAND INSTITUTE FOR CLINICAL'; 
	update ad.locs set fac_proc = replace(fac_proc, 'MENTER DERMATOLOGY', 'Menter Dermatology') where fac_proc ~ 'MENTER DERMATOLOGY'; 
	update ad.locs set fac_proc = replace(fac_proc, 'POST GRADUATE INSTITUTE OF DENTAL SCIENCES', 'Post Graduate Institute of Dental Sciences') 
	where fac_proc ~ 'POST GRADUATE INSTITUTE OF DENTAL SCIENCES'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT DE CANCEROLOGIE DE LA LOIRE', 'Institut de Cancerologie de la Loire') 
	where fac_proc ~ 'INSTITUT DE CANCEROLOGIE DE LA LOIRE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'SOMNI BENE INSTITUT FUR MEDIZIMISCHE FORSCHUNG & SCHLAFMEDEZIN', 'Somni Bene Institut fur Medizimische Forshung & Schlafmedezin') 
	where fac_proc ~ 'SOMNI BENE INSTITUT FUR MEDIZIMISCHE FORSCHUNG & SCHLAFMEDEZIN'; 
	update ad.locs set fac_proc = replace(fac_proc, 'URAL INSTITUTE OF CARDIOLOGY', 'Ural Institute of Cariology') where fac_proc ~ 'URAL INSTITUTE OF CARDIOLOGY'; 

	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTO', 'Instituto') where fac_proc ~ 'INSTITUTO'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUTE', 'Institute') where fac_proc ~ 'INSTITUTE'; 
	update ad.locs set fac_proc = replace(fac_proc, 'INSTITUT', 'Institut') where fac_proc ~ 'INSTITUT'; 

    */
        
    Ok(())
}
