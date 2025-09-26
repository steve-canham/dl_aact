use super::locs_utils::{replace_in_fac_proc, execute_sql, remove_regexp_from_fac_proc,
                        replace_list_items_with_target, add_zzz_prefix_to_list_items, 
                        remove_leading_char_in_fac_proc, remove_trailing_char_in_fac_proc,
                        replace_list_items_with_capitalised, replace_list_items_with_lower_case};

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
    
    replace_in_fac_proc("[", "(", "k", true, "", pool).await?; 
    replace_in_fac_proc("]", ")", "k", true, "", pool).await?; 
    replace_in_fac_proc("{", "(", "k", true, "", pool).await?; 
    replace_in_fac_proc("}", ")", "k", true, "", pool).await?;

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

    info!("Replacing non-utf with utf equivalents...");
    info!("");

    // Start by clearing the decks by removing these (apparently) redundant codes

    replace_in_fac_proc( "â€�", "", "r", true, "", pool).await?; 
    replace_in_fac_proc( "ï€£3", "", "r", true, "", pool).await?; 

    // Then do the following replacements, involving '�', often standing in
    // for several letters as well as different letters in different contexts
   
	replace_in_fac_proc("Center �� Carmichael", "Center Carmichael", "r", true, "", pool).await?; 
	replace_in_fac_proc("Zak��ad", "Zakład", "r", true, "", pool).await?; 
	replace_in_fac_proc("H�pital", "Hôpital", "r", true, "", pool).await?; 
	replace_in_fac_proc("Universit�Tsklinikum", "Universitätsklinikum", "r", true, "", pool).await?; 
	replace_in_fac_proc("Investigaci�n Cl�nica", "Investigación Clínica", "r", true, "", pool).await?; 
	replace_in_fac_proc("Centre L� B�rd", "Centre Léon Bérard", "r", true, "", pool).await?; 
	replace_in_fac_proc("Universit�Tsmedizin", "Universitätsmedizin", "r", true, "", pool).await?; 
	replace_in_fac_proc("C�te", "Côte", "r", true, "", pool).await?; 
	
    replace_in_fac_proc("Besan�on", "Besançon", "r", true, "", pool).await?; 
	replace_in_fac_proc("D�Hebron", "D’Hebron", "r", true, "", pool).await?; 
	replace_in_fac_proc("H�al Saint-Antoine", "Hôpital Saint-Antoine", "r", true, "", pool).await?; 
	replace_in_fac_proc("H�Pital Universitaire Piti�-Salp�Tri�Re", "Hôpital Universitaire Pitié-Salpêtrière", "r", true, "", pool).await?; 
	replace_in_fac_proc("Pitie-Salpetri�re", "Pitié-Salpêtrière", "r", true, "", pool).await?; 
	replace_in_fac_proc("Antoine Becl�re", "Antoine-Béclère", "r", true, "", pool).await?; 
	replace_in_fac_proc("Servi�os", "Serviços", "r", true, "", pool).await?; 
	replace_in_fac_proc("SE�Ra", "Señora", "r", true, "", pool).await?; 

	replace_in_fac_proc("H�tel", "Hôtel", "r", true, "", pool).await?; 
	replace_in_fac_proc("General Yag�", "General Yagüe", "r", true, "", pool).await?; 
	replace_in_fac_proc("Gregorio Mara�on", "Gregorio Marañón", "r", true, "", pool).await?; 
	replace_in_fac_proc("Est�ca do", "Estética do", "r", true, "", pool).await?; 
	replace_in_fac_proc("\"Dermed�", "\"Dermed", "r", true, "", pool).await?; 
	replace_in_fac_proc("Spitalul Jude�ean de Urgen�a dr. Constantin Opri�", "Spitalul Judeţean de Urgenţă Dr.Constantin Opriş", "r", true, "", pool).await?; 
	replace_in_fac_proc("zaboliavania�", "zaboliavania", "r", true, "", pool).await?; 
    replace_in_fac_proc("�L. E A. Ser�Gnoli�", "L. e A. Seragnoli", "r", true, "", pool).await?; 

    // Question marks in name demand slightly different tack
    replace_in_fac_proc("Szpitale Wojew�Dzkie W Gdyni Sp�?Ka Z Ograniczon? Odpowiedzialno?Ci?", "Szpitale Wojewódzkie w Gdyni Sp. z o.o", "k", true, "", pool).await?; 

    info!("");
    Ok(())
}

pub async fn repair_non_ascii_2(pool: &Pool<Postgres>) -> Result<(), AppError> {  

    // A few strange ones need to be dealt with individually

    replace_in_fac_proc( "Ã-rebro", "Örebro", "r", true, "", pool).await?; 
    replace_in_fac_proc( " CittÃ", " Città", "r", true, "", pool).await?; 
    replace_in_fac_proc( " UnitÃ", " Unità", "r", true, "", pool).await?; 
    replace_in_fac_proc( "LaÃnnec", "Laënnec", "r", true, "", pool).await?; 
    replace_in_fac_proc( "LaÃ\"nnec", "Laënnec", "r", true, "", pool).await?; 
    
    replace_in_fac_proc( " CatalÃ", " Català", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Son LlÃ tzer", "Son Llàtzer", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã OK", "Á OK", "r", true, "", pool).await?; 
    replace_in_fac_proc( "ParkinsonÃÂ¿s", "Parkinson’s", "r", true, "", pool).await?; 
    replace_in_fac_proc( "OncologÃƒÂ-a", "Oncología", "r", true, "", pool).await?; 
    replace_in_fac_proc( "LÃon BÃrard Centre RÃgional", "Léon Bérard Centre Régional", "r", true, "", pool).await?; 
    replace_in_fac_proc( "GraubÃnden", "Graubünden", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Fundaã§Ã£O", "Fundação", "r", true, "", pool).await?; 
    replace_in_fac_proc( "RenÃ ", "René ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Presidentâ€™s ", "President’s ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Oâ€™Neil", "O’Neil", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Marii SkÅ''odowskiej-Curie â€\" PaÅ\"stwowy Instytut", "Marii Skłodowskiej-Curie, Państwowy Instytut","r", true, "", pool).await?; 
    replace_in_fac_proc( "Researchâ€\" ", "Research, ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "the â€œHealth", "the Health", "r", true, "", pool).await?; 
    replace_in_fac_proc( " â€œ", ", ", "r", true, "", pool).await?; 
    replace_in_fac_proc( " â€\" ", ", ", "r", true, "", pool).await?; 
    info!("");

    // Then do these - a few for each of them
	
    replace_in_fac_proc( "Ã©", "é", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã´", "ô", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¨", "è", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã-", "í", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã§", "ç", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã£", "ã", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¡", "á", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¶", "ö", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¤", "ä", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã³", "ó", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¼", "ü", "r", true, "", pool).await?; 
    replace_in_fac_proc( "ã¼", "ü", "r", true, "", pool).await?; 
    replace_in_fac_proc( "ÃŸ", "ß", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã±", "ñ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ãª", "ê", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Ã¢", "â", "r", true, "", pool).await?; 
    
    info!("");
    Ok(())
}
    
pub async fn repair_non_ascii_3(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_in_fac_proc( "CARITï¿½ DI", "CARITÀ DI", "r", true, "", pool).await?; 
    replace_in_fac_proc( "FRANï¿½OIS", "FRANÇOIS", "r", true, "", pool).await?; 
    replace_in_fac_proc( "LIï¿½GE", "LIÈGE", "r", true, "", pool).await?; 
    replace_in_fac_proc( "UNIVERSITï¿½TSMEDIZIN", "UNIVERSITÄTSMEDIZIN", "r", true, "", pool).await?; 
    replace_in_fac_proc( "UNIVERSITï¿½TSKLINIKUM", "UNIVERSITÄTSKLINIKUM", "r", true, "", pool).await?; 
    replace_in_fac_proc( "Hï¿½PITAL", "HÔPITAL", "r", true, "", pool).await?; 
    replace_in_fac_proc( "UNIVERSITï¿½", "UNIVERSITÀ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "ST. MARYï¿½S HOSPITAL", "ST. MARY’S HOSPITAL", "r", true, "", pool).await?; 
    replace_in_fac_proc( " ï¿½ ", " - ", "r", true, "", pool).await?; 
    replace_in_fac_proc( "SZPITALE WOJEWï¿½DZKIE W GDYNI SPï¿½LKA Z OGRANICZONA ODPOWIEDZIALNOSCIA", "Szpitale Wojewódzkie w Gdyni Sp. z o.o.", "r", true, "", pool).await?; 


    replace_in_fac_proc("Children¿s", "Children’s", "r", true, "", pool).await?; 
    replace_in_fac_proc("D¿Hebron", "D’Hebron", "r", true, "", pool).await?; 
    replace_in_fac_proc("Quinta D¿Or", "Quinta D’Or", "r", true, "", pool).await?; 
    replace_in_fac_proc("Hospital ¿1", "Hospital No.1", "r", true, "", pool).await?; 
    replace_in_fac_proc("6¿ City", "No.6 City", "r", true, "", pool).await?; 
    replace_in_fac_proc("Hospital ¿ 442", "Hospital No.442", "r", true, "", pool).await?; 
    replace_in_fac_proc("Institute¿Downriver", "Institute - Downriver", "r", true, "", pool).await?; 
    replace_in_fac_proc("Rafa¿", "Rafał", "r", true, "", pool).await?; 
    replace_in_fac_proc(" ¿National Medical Research Oncology Centre named after N.N.", ", National Medical Research Center of Oncology named after N.N. Petrov", "r", true, "", pool).await?; 
    replace_in_fac_proc("ZespAA GruAicy i ChorAb PA¿uc", "Zespół Gruźlicy i Chorób Płuc", "r", true, "", pool).await?; 
    replace_in_fac_proc("¿KardioDent¿", "KardioDent", "r", true, "", pool).await?; 
    replace_in_fac_proc("¿Sveti Naum¿", "Sveti Naum", "r", true, "", pool).await?; 
    replace_in_fac_proc("¿Promedicus¿", "Promedicus", "r", true, "", pool).await?; 
    replace_in_fac_proc("¿Attikon¿", "Attikon", "r", true, "", pool).await?; 
    replace_in_fac_proc("¿Sf. Apostol Andrei¿", "Sf. Apostol Andrei", "r", true, "", pool).await?; 
    replace_in_fac_proc("Charleroi ¿ Site Imtr", "Charleroi - Site IMTR", "r", true, "", pool).await?;
    replace_in_fac_proc("Sk¿odowskiej-Curie", "Skłodowska-Curie", "r", true, "", pool).await?; 
    replace_in_fac_proc("Zak¿ad", "Zakład", "r", true, "", pool).await?; 
    replace_in_fac_proc("Vitamed -Ga¿aj I", "Vitamed Gałaj i", "r", true, "", pool).await?; 
    replace_in_fac_proc("Region Â¿Sverdlovsk", "Region, Sverdlovsk", "r", true, "", pool).await?; 
    replace_in_fac_proc("Oddzia¿", "Oddział", "r", true, "", pool).await?; 
    replace_in_fac_proc(" ¿Region Clinical Hospital #3¿", ", Region Clinical Hospital #3", "r", true, "", pool).await?; 
    replace_in_fac_proc(" ¿ ",  " - ", "r", true, "", pool).await?; 

    info!("");
    Ok(())
}


pub async fn remove_single_quotes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_in_fac_proc( "&amp;", "&", "k", true, "", pool).await?; 
    // The &amp; update has to be done twice
    replace_in_fac_proc( "&amp;", "&", "k", true, "", pool).await?; 
    replace_in_fac_proc( "&quot;", "", "k", true, "", pool).await?; 
    replace_in_fac_proc( "&#39;", "’", "k", true, "", pool).await?; 
    info!("");
    Ok(())
}

pub async fn  remove_double_quotes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_in_fac_proc( "\"", "", "k", true, "", pool).await?; 
    replace_in_fac_proc( "&#34;", "", "k", true, "", pool).await?; 
    replace_in_fac_proc( "''''", "", "k", true, "", pool).await?; 
    info!("");
    Ok(())
}


pub async fn remove_double_commas(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_in_fac_proc( " ,,", " ", "k", true, "", pool).await?; 
    replace_in_fac_proc( ",, ", ", ", "k", true, "", pool).await?; 

    replace_in_fac_proc( ",,", "", "fac_proc like '%,,' or fac_proc like ',,%'", true, "from beginning or end of name", pool).await?; 
    replace_in_fac_proc( ",,", ", ", "fac_proc ~ '[A-za-z]+,,[A-za-z]+'", true, "when directly between text", pool).await?; 
    
    info!("");        
    Ok(())
}

pub async fn process_apostrophes(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Single apostrophe processing notmally converts to a right single quote (RSQ)
	// unless at the start of a line or after a space

	// The first group are a small cluster of Dutch names starting with 't or 's 
	// (single letter contractions of words for 'the' and 'of the'). 
    // The apostrophes are replaced by a right single quote.

    replace_in_fac_proc( "''", "’", r#"fac_proc ~ '^''[a-zA-Z] ' or fac_proc ~ 's-Hertogenbosch' 
    or fac_proc ~ 'Arnhem ''S Radiotherapeutisc'"#, false, "apostrophes before single letters replaced by RSQ", pool).await?; 
     
    // a second group deals with a typo (especially common with some Chinese hospitals)

    replace_in_fac_proc( " ''s ", "’s ", r#"fac_proc ~ ' ''s Hospit' or fac_proc ~ ' ''s Liberat'
	or fac_proc ~ ' ''s Research'"#, false, "apostrophes before space and s, after a word, space, corrected", pool).await?; 

    // A third group has another type that seems to involve a spurious 's, which can be removed

    replace_in_fac_proc( " ''s ", " ", r"fac_proc ~ 'Einstein ''s \(IIEP\)$'", false, "spurious apostrophes and s removed", pool).await?; 
   
    // in almost all other cases the presence of '% ''%' indicates a pair of quotes around a name. They can both be removed.
    // Otherwise the apostrophe should be replaced by a right singlr quote.

    replace_in_fac_proc( "''", "", "fac_proc ~ ' '''", false, "apostrophes removed when found after a space", pool).await?; 
    replace_in_fac_proc( "''", "’", "r", false, "apostrophes replaced by right single quote", pool).await?; 
   
    info!("");
    Ok(())
}

pub async fn process_upper_ticks(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // The character is U+00b4. Seems to be used as an apostrophe in some 
    // settings (esp. Spanish records), and can be processed in a similar way

    // this odd one needs to be done first

    replace_in_fac_proc( "d ´Hebron", "d’Hebron", "r", true, "", pool).await?; 
   
    // U+00b4 at the start of a line or after a space - indicates all can be removed from the line.

    replace_in_fac_proc( "´", "´", "fac_proc ~ '^´' or fac_proc ~ ' ´'", true, 
    "when found at beginning or after a space", pool).await?; 

    // then the remainder - straightforward replacement (apart from one odd one that needs a )
 
    replace_in_fac_proc( "´", "’", "r", true, "", pool).await?; 

    info!("");
    Ok(())
}

pub async fn remove_leading_trailing_odd_chars(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    remove_leading_char_in_fac_proc("-", true, pool).await?;
    remove_leading_char_in_fac_proc(".", true, pool).await?;
    remove_leading_char_in_fac_proc(",", true, pool).await?;
    remove_leading_char_in_fac_proc(":", true, pool).await?;
    remove_leading_char_in_fac_proc(";", true, pool).await?;
    remove_leading_char_in_fac_proc("&", true, pool).await?;
    remove_leading_char_in_fac_proc("•", true, pool).await?;
    remove_leading_char_in_fac_proc("_", true, pool).await?;

    remove_trailing_char_in_fac_proc("-", true, pool).await?;
    remove_trailing_char_in_fac_proc(".", true, pool).await?;
    remove_trailing_char_in_fac_proc(",", true, pool).await?;
    remove_trailing_char_in_fac_proc(":", true, pool).await?;
    remove_trailing_char_in_fac_proc(";", true, pool).await?;
    remove_trailing_char_in_fac_proc("&", true, pool).await?;
    remove_trailing_char_in_fac_proc("’", true, pool).await?;
    
    replace_in_fac_proc( "!", "1", "fac_proc ~ '^!'", true, "when occuring at the start of the name", pool).await?; 

    //let sql = r#"update ad.locs set fac_proc = replace(fac_proc, '!', '1') where fac_proc ~ '^!'; "#;
    //execute_sql(sql, pool).await?.rows_affected();
 
    let sql = r#"update ad.locs set fac_proc = trim(fac_proc) where fac_proc ~ '^ ' or fac_proc ~ ' $';  "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records with spaces at beginning or end trimmed", res); 

    info!("");
    Ok(())

}

pub async fn remove_underscores(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Need to do these particular situations first

    replace_in_fac_proc("_", "", "fac_proc ~ '[A-Za-z0-9#-]_ '", true, "when directly following text and before a space", pool).await?; 
    replace_in_fac_proc("_", "", "fac_proc ~ ' _[A-Za-z0-9#-]'", true, "when directly preceding text after a space", pool).await?; 
    replace_in_fac_proc("_", " ", "fac_proc ~ '[A-Za-z0-9#-]_[A-Za-z0-9#-]'", true, "when directly between text", pool).await?; 
    replace_in_fac_proc("_", "", "fac_proc ~ '_$'", true, "when at the end of the name", pool).await?; 
   
    // Strightforward replacements

	replace_in_fac_proc(" _ ", " ", "r", true, "", pool).await?; 
	replace_in_fac_proc("._", ". ", "r", true, "", pool).await?; 
	replace_in_fac_proc("__", " ", "r", true, "", pool).await?; 

    // Remainder 

	replace_in_fac_proc("_", " ", "r", true, "", pool).await?; 

    info!("");    
    Ok(())
}


pub async fn improve_comma_spacing(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // First deal with these few specific anomalies.

    replace_in_fac_proc(",.", ",", "fac_proc like '%Policlinico A,. Gemelli%' or fac_proc like '%Genetics Unit,. Royal Manchester%'", false, "", pool).await?; 
    replace_in_fac_proc(",.", ".,", "fac_proc like '%Co Ltd,. Haneda%'", false, "", pool).await?; 
    
	// Then do the general insertion of a space after a comma next to text.
	    
    replace_in_fac_proc(",", ", ", r"fac_proc ~ ',[A-Za-z0-9\(\)#]'", false, "spaces inserted after commas which were directly before text", pool).await?; 

    // The process above adds many spurious spaces, along with existing anomalies of the kinds below

    replace_in_fac_proc(",  ", ", ", "r", true, "", pool).await?; 
	replace_in_fac_proc(" , ", ", ", "r", true, "", pool).await?; 
    
    info!("");  
    Ok(())
}


pub async fn improve_bracket_spacing(pool: &Pool<Postgres>) -> Result<(), AppError> {  
        
    // Right bracket

    replace_in_fac_proc(")", ") ", r"fac_proc ~ '\)[A-Za-z0-9#-]'", false, "spaces inserted after right bracket directly before text", pool).await?; 
    replace_in_fac_proc(")   ", ") ", r"fac_proc ~ '\)  '", true, "", pool).await?; 
    replace_in_fac_proc(" ) ", ") ", r"fac_proc ~ ' \) '", true, "", pool).await?; 
   
    // Left bracket

    replace_in_fac_proc("(", " (", r"fac_proc ~ '[A-Za-z0-9#.\)-]\(' and fac_proc !~ '\([a-z]\)'", false, "spaces inserted before left bracket directly after text", pool).await?; 
    replace_in_fac_proc("  (", " (", r"fac_proc ~ '  \('", true, "", pool).await?; 
    replace_in_fac_proc(" ( ", " (", r"fac_proc ~ ' \( '", true, "", pool).await?; 
   
    info!("");
    Ok(())
}


pub async fn remove_initial_numbering(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    remove_regexp_from_fac_proc(r"^[0-9]{1,2}\. ", r"^[0-9]{1,2}\. ", "initial numbering removed", pool).await?;
    remove_regexp_from_fac_proc(r"^[a-e]{1}\. ", r"^[a-e]{1}\. ", "initial lettering removed", pool).await?;

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


pub async fn correct_place_names(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    replace_in_fac_proc("germany", "Germany", "r", true, "", pool).await?; 
    replace_in_fac_proc(" france", " France", "r", true, "", pool).await?; 
    replace_in_fac_proc(" russian", " Russian", "r", true, "", pool).await?; 
    replace_in_fac_proc(" china", " China", "r", true, "", pool).await?; 
    replace_in_fac_proc("turkey", "Turkey", "r", true, "", pool).await?; 
    replace_in_fac_proc("israel", "Israel", "r", true, "", pool).await?; 
    
    replace_in_fac_proc("indiana", "Indiana", "r", true, "", pool).await?; 
    replace_in_fac_proc(" india", " India", "r", true, "", pool).await?; 
    replace_in_fac_proc("pakistan", "Pakistan", "r", true, "", pool).await?; 
    replace_in_fac_proc("california", "California", "r", true, "", pool).await?; 
    replace_in_fac_proc("baja ", "Baja ", "r", true, "", pool).await?; 
    replace_in_fac_proc("istanbul", "Istanbul", "r", true, "", pool).await?; 

    replace_in_fac_proc("cairo", "Cairo", "r", true, "", pool).await?; 
    replace_in_fac_proc("cannes", "Cannes", "r", true, "", pool).await?; 
    replace_in_fac_proc("asyut", "Assiut", "r", true, "", pool).await?; 
    replace_in_fac_proc("assiut", "Assiut", "r", true, "", pool).await?; 
    replace_in_fac_proc("assuit", "Assiut", "r", true, "", pool).await?; 
    replace_in_fac_proc("aarhus", "Aarhus", "r", true, "", pool).await?; 

    replace_in_fac_proc("beijing", "Beijing", "r", true, "", pool).await?; 
    replace_in_fac_proc("fayoum", "Fayoum", "r", true, "", pool).await?; 
    replace_in_fac_proc("nantes", "Nantes", "fac_proc ~ ' nantes' or fac_proc ~ '^nantes'", true, "", pool).await?; 
    replace_in_fac_proc("korea", "Korea", "r", true, "", pool).await?; 
    replace_in_fac_proc("marseille", "Marseille", "r", true, "", pool).await?; 
    replace_in_fac_proc("nice", "Nice", "fac_proc ~ '^nice' or fac_proc ~ ' nice'", true, "", pool).await?; 

    replace_in_fac_proc("radboud", "Radboud", "r", true, "", pool).await?; 
    replace_in_fac_proc("rennes", "Rennes", "r", true, "", pool).await?; 
    replace_in_fac_proc("ruijin", "Ruijin", "r", true, "", pool).await?; 
    replace_in_fac_proc("sheffield", "Sheffield", "r", true, "", pool).await?; 
    replace_in_fac_proc("shanghai", "Shanghai", "fac_proc ~ '^shanghai' or fac_proc ~ ' shanghai'", true, "", pool).await?; 
    replace_in_fac_proc("shandong", "Shandong", "fac_proc ~ '^shandong' or fac_proc ~ ' shandong'", true, "", pool).await?; 

    replace_in_fac_proc("shengjing", "Shengjing", "r", true, "", pool).await?; 
    replace_in_fac_proc("shiraz", "Shiraz", "r", true, "", pool).await?; 
    replace_in_fac_proc("sydney", "Sydney", "r", true, "", pool).await?; 
    replace_in_fac_proc("tel aviv", "Tel Aviv", "r", true, "", pool).await?; 
    replace_in_fac_proc("zhongshan", "Zzhongshan", "r", true, "", pool).await?; 
    replace_in_fac_proc("zhujiang", "Zhujiang", "r", true, "", pool).await?; 

    replace_in_fac_proc("zhengzhou", "Zhengzhou", "r", true, "", pool).await?; 
    replace_in_fac_proc("brest", "Brest", "fac_proc ~ '^brest' or fac_proc ~ ' brest'", true, "", pool).await?; 
    replace_in_fac_proc("caen", "Caen", "fac_proc ~ '^caen' or fac_proc ~ ' caen'", true, "", pool).await?; 
    replace_in_fac_proc("izmir", "Izmir", "r", true, "", pool).await?; 
    replace_in_fac_proc("miami", "Miami", "fac_proc ~ '^miami' or fac_proc ~ ' miami'", true, "", pool).await?; 
  
    info!("");  
    Ok(())
}


pub async fn correct_lower_case_beginnings(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // There are some anomalous lower case beginnings for some facility names
	// On the other hands some companies / organisationsd genuinely have a lower case start
	// and some are lower case because of missing first letters - therefore cannot do a blanket 
	// capitalisation of the first letter.

	// Those below involve more than simple capitalisation 

    let sql = r#"update ad.locs set fac_proc = 'The '||substring(fac_proc, 5) where fac_proc ~'^the ';  "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had initial 'the ' replaced by 'The '", res); 

    replace_in_fac_proc("he ", "The ", "b", true, "", pool).await?; 

    replace_in_fac_proc("department", "Department", "b", true, "", pool).await?; 
    replace_in_fac_proc("dep ", "Department ", "b", true, "", pool).await?; 
    replace_in_fac_proc("dep.", "Department", "b", true, "", pool).await?; 
    replace_in_fac_proc("dept", "Department of", "b", true, "", pool).await?; 
    replace_in_fac_proc("dEP ", "Department of", "b", true, "", pool).await?; 

    replace_in_fac_proc("faculty of medicine", "Faculty of Medicine", "r", true, "", pool).await?; 
    replace_in_fac_proc("faculty of Medicine", "Faculty of Medicine", "r", true, "", pool).await?; 
    replace_in_fac_proc("faculty of dentistry", "Faculty of Dentistry", "r", true, "", pool).await?; 
    replace_in_fac_proc("faculty of Dentistry", "Faculty of Dentistry", "r", true, "", pool).await?; 
    replace_in_fac_proc("faculty ", "Faculty ", "r", true, "", pool).await?; 

    replace_in_fac_proc("cLERMONT fERRAND", "Clermont Ferrand", "b", true, "", pool).await?; 
    replace_in_fac_proc("fERNANDO bARATA", "Fernando Barata", "b", true, "", pool).await?; 
	
	replace_in_fac_proc("at ","", "b", true, "", pool).await?; 
	replace_in_fac_proc("ap-HM","AP-HM", "b", true, "", pool).await?; 
	replace_in_fac_proc("aQua", "Aqua", "b", true, "", pool).await?;  
	replace_in_fac_proc("azienda", "Azienda", "b", true, "", pool).await?; 
	replace_in_fac_proc("zienda", "Azienda", "b", true, "", pool).await?; 
	replace_in_fac_proc("az", "AZ", "b", true, "", pool).await?; 
	replace_in_fac_proc("ain shams", "Ain Shams", "b", true, "", pool).await?;  
	replace_in_fac_proc("am ", "", "b", true, "", pool).await?;  
	replace_in_fac_proc("an Antonio", "San Antonio ", "b", true, "", pool).await?;  
	replace_in_fac_proc("a.o.", "A.o. ", "b", true, "", pool).await?; 
	
	replace_in_fac_proc("bBston", "Boston", "b", true, "", pool).await?; 
	replace_in_fac_proc("c/o ", "", "b", true, "", pool).await?;
	replace_in_fac_proc("C/O ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("cit ", "CIT ", "b", true, "", pool).await?;
	replace_in_fac_proc("coi ", "COI ", "b", true, "", pool).await?; 

	replace_in_fac_proc("de’ Montmorency", "de’Montmorency", "b", true, "", pool).await?; 
	replace_in_fac_proc("d’Hebron", "Vall d’Hebron", "b", true, "", pool).await?; 
	replace_in_fac_proc("all D’Hebron", "Vall d’Hebron", "b", true, "", pool).await?;
	replace_in_fac_proc("dVeterans", "Veterans", "b", true, "", pool).await?; 
	replace_in_fac_proc("dba ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("d-b-a ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("ddC ", "DDC", "b", true, "", pool).await?; 
	replace_in_fac_proc("dGd ", "DGD", "b", true, "", pool).await?; 
	
	replace_in_fac_proc("e Clatterbridge", "Clatterbridge", "b", true, "", pool).await?;
	replace_in_fac_proc("ezione", "Sezione", "b", true, "", pool).await?; 
	replace_in_fac_proc("eatson", "Beatson", "b", true, "", pool).await?;  
	replace_in_fac_proc("econd", "Second", "b", true, "", pool).await?; 
	replace_in_fac_proc("from the o", "O", "b", true, "", pool).await?; 
	replace_in_fac_proc("f the ", "The", "b", true, "", pool).await?; 
	replace_in_fac_proc("hildren", "Children", "b", true, "", pool).await?; 
	replace_in_fac_proc("ialysis", "Dialysis", "b", true, "", pool).await?;  
	replace_in_fac_proc("ianjin", "Tianjin", "b", true, "", pool).await?; 
	replace_in_fac_proc("i ASL", "ASL", "b", true, "", pool).await?;  
	replace_in_fac_proc("iBS", "ibs", "b", true, "", pool).await?; 

	replace_in_fac_proc("i Can ", "ICAN ", "b", true, "", pool).await?; 
	replace_in_fac_proc("iCan ", "ICAN ", "b", true, "", pool).await?;
	replace_in_fac_proc("I Can ", "ICAN ", "b", true, "", pool).await?;  
	replace_in_fac_proc("I CAN ", "ICAN ", "b", true, "", pool).await?; 
	replace_in_fac_proc("inonuU", "İnönü Üniversitesi", "b", true, "", pool).await?;  
	replace_in_fac_proc("icm ", "ICM ", "b", true, "", pool).await?; 
	replace_in_fac_proc("ifo ", "IFO ", "b", true, "", pool).await?; 
	replace_in_fac_proc("i. ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("ii. ", "", "b", true, "", pool).await?;  
	replace_in_fac_proc("i Mei ", "Chi Mei ", "b", true, "", pool).await?;  
	
	replace_in_fac_proc("insaf", "INSAF", "b", true, "", pool).await?; 
	replace_in_fac_proc("irccs", "IRCCS", "b", true, "", pool).await?; 
	replace_in_fac_proc("ir Charles", "Sir Charles", "b", true, "", pool).await?; 
    replace_in_fac_proc("iverpool", "Liverpool", "b", true, "", pool).await?;  
	replace_in_fac_proc("ivision", "Division", "b", true, "", pool).await?; 
	replace_in_fac_proc("institut", "Institut", "b", true, "", pool).await?; 
	replace_in_fac_proc("lnstitut", "Institut", "b", true, "", pool).await?; 
	replace_in_fac_proc("istituto", "Istituto", "b", true, "", pool).await?; 
	replace_in_fac_proc("lstituto", "Istituto", "b", true, "", pool).await?; 
	replace_in_fac_proc("nstituto", "Instituto", "b", true, "", pool).await?; 
	replace_in_fac_proc("stitute", "Institute", "b", true, "", pool).await?;
	replace_in_fac_proc("stituto", "INstituto", "b", true, "", pool).await?;  

	replace_in_fac_proc("epartment", "Department", "b", true, "", pool).await?;
	replace_in_fac_proc("entre ", "Centre ", "b", true, "", pool).await?;
	replace_in_fac_proc("entrum Med", "Centrum Med", "b", true, "", pool).await?;  
	replace_in_fac_proc("ervice", "Service", "b", true, "", pool).await?;
	replace_in_fac_proc("est China", "West China", "b", true, "", pool).await?; 
	
	replace_in_fac_proc("jmf", "JMF", "b", true, "", pool).await?; 
	replace_in_fac_proc("lcahn", "Icahn", "b", true, "", pool).await?;
	replace_in_fac_proc("linical", "Clinical", "b", true, "", pool).await?; 
	replace_in_fac_proc("chu", "CHU", "b", true, "", pool).await?;  
	replace_in_fac_proc("niversity", "University", "b", true, "", pool).await?; 
	replace_in_fac_proc("nvestigative", "Investigative", "b", true, "", pool).await?; 
	replace_in_fac_proc("llege", "College", "b", true, "", pool).await?;  
	replace_in_fac_proc("ll ", "", "b", true, "", pool).await?;
	replace_in_fac_proc("ospedale", "Ospedale", "b", true, "", pool).await?;
	replace_in_fac_proc("ospdale", "Ospedale", "b", true, "", pool).await?;
	replace_in_fac_proc("ospdali", "Ospedale", "b", true, "", pool).await?; 
	replace_in_fac_proc("sanofi-aventi", "Sanofi-Aventi", "b", true, "", pool).await?; 
	replace_in_fac_proc("maha sadek", "Maha Sadeks", "b", true, "", pool).await?;

	replace_in_fac_proc("lnamdar", "Inamdar", "b", true, "", pool).await?;  
	replace_in_fac_proc("lndraprastha", "Indraprastha", "b", true, "", pool).await?; 
	replace_in_fac_proc("lnje", "Inje", "b", true, "", pool).await?;  
	replace_in_fac_proc("lnstytut", "Instytut", "b", true, "", pool).await?;
	replace_in_fac_proc("lntermed", "Intermed", "b", true, "", pool).await?; 
	replace_in_fac_proc("lnvestigational", "Ilnvestigational", "b", true, "", pool).await?; 
	replace_in_fac_proc("lOP", "IOP", "b", true, "", pool).await?; 
	replace_in_fac_proc("lRCCS", "IRCCS", "b", true, "", pool).await?;  
	replace_in_fac_proc("lrmandade", "Irmandade", "b", true, "", pool).await?;
	replace_in_fac_proc("lvanovo", "Ivanovo", "b", true, "", pool).await?;
	replace_in_fac_proc("ndiana", "Indiana", "b", true, "", pool).await?; 
	replace_in_fac_proc("ngShanghai", "Shanghai", "b", true, "", pool).await?;  
	replace_in_fac_proc("nineth", "Ninth", "b", true, "", pool).await?; 
	replace_in_fac_proc("nited", "United", "b", true, "", pool).await?;  
	replace_in_fac_proc("niversit", "Universit", "b", true, "", pool).await?;  
	replace_in_fac_proc("nstitut", "Institut", "b", true, "", pool).await?; 

	replace_in_fac_proc("o ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("of ", "", "b", true, "", pool).await?; 
	replace_in_fac_proc("ongji Hospital", "Tongji Hospital", "b", true, "", pool).await?;  
	replace_in_fac_proc("omplejo", "Complejo", "b", true, "", pool).await?;  
	replace_in_fac_proc("ordan", "Jordan", "b", true, "", pool).await?;  
	replace_in_fac_proc("pitalul", "Spitalul", "b", true, "", pool).await?; 
	replace_in_fac_proc("psrd", "PSRD", "b", true, "", pool).await?; 
	replace_in_fac_proc("qeii", "QEII", "b", true, "", pool).await?;  
	replace_in_fac_proc("r. Horst", "Dr. Horst", "b", true, "", pool).await?;  
	replace_in_fac_proc("rivat", "Privat", "b", true, "", pool).await?;
	replace_in_fac_proc("rmandade", "Irmandade", "b", true, "", pool).await?; 
	replace_in_fac_proc("sms", "SMS", "b", true, "", pool).await?; 

    replace_in_fac_proc("nOvum", "Novum", "b", true, "", pool).await?; 
	replace_in_fac_proc("spedale", "Ospedale", "b", true, "", pool).await?; 
	replace_in_fac_proc("spedali", "Ospedali", "b", true, "", pool).await?;  
	replace_in_fac_proc("stanbul", "Istanbul", "b", true, "", pool).await?;  
	replace_in_fac_proc("st China", "West China", "b", true, "", pool).await?;  
	replace_in_fac_proc("suAzio", "SUAZIO", "b", true, "", pool).await?; 
	replace_in_fac_proc("td ", "", "b", true, "", pool).await?;  
	replace_in_fac_proc("tlc", "TLC", "b", true, "", pool).await?; 
	replace_in_fac_proc("uc davis", "UC Davis", "b", true, "", pool).await?; 
	replace_in_fac_proc("uijin", "Ruijin", "b", true, "", pool).await?;  
	replace_in_fac_proc("uz", "UZ", "b", true, "", pool).await?;  
	replace_in_fac_proc("vzw", "VZW", "b", true, "", pool).await?; 

    info!(""); 

	// Provide temporary protection from blanket capitalisation by adding "ZZZ" to the beginning of these genuinely lower case terms

    let ws = vec!["aai", "aTyr", "analyze & realize", "bioskin", "cCare", "cyberDERM", "de ", "dgd", "dermMedica", "doTERRA", "eMax", "eMKa", "emovis"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["eStudy", "e-Study", "hVIVO", "i9 ", "iBiomed","iCCM", "ifi", "ikfe", "hVIVO", "inVentiv", "iResearch"];
	add_zzz_prefix_to_list_items(&ws, pool).await?; 

    let ws = vec!["nTouch", "bitop", "amO", "bioLytical", "daacro", "eResearch Technology", "eRT", "eThekwini", "eSSe", "de’Montmorency", "dell’", "cCARE"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["aCROnordic", "dTIP", "duPont", "eCare", "eCast", "eCommunity", "eps", "eStöd", "eStod", "estudy site", "eSupport", "framol-med"];
	add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec![ "go:h", "goMedus", r"*g\.Sund", r"g\.tec", "http", "hyperCORE", "i3", "ibs", "icddr", "iD3", "iConquerMS", "ideSHi"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["iDia", "ife", "iHealth", "iHope", "iKardio", "i Kokoro", "iMD", "iMedica", "iMED", "iMindU", "imland", "inContAlert"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["iNeuro", "iOMEDICO", "*ipb", "i-Research", "iSpecimen", "iSpine", "iThera", "iuvo", "ivWatch", "jMOG", "kbo", "kConFab"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["kfgn", "medamed", "medbo", "medicoKIT", "mediPlan", "medius", "mediX", "med.ring", "m&i", "mind ", "ms²", "myBETAapp"];
	add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["my mhealth", "nordBLICK", "physIQ", "pioh", "play2PREVENT", "*proDerm", "pro mente ", "proSANUS", "pro scientia", "radprax", "rTMS"];
    add_zzz_prefix_to_list_items(&ws, pool).await?; 

	let ws = vec!["selectION", "suitX", "tecura", "terraXCube", "toSense", "uCT960+", "uEXPLORER", "uniQure", "xCures", "ze:ro", "zibp"]; 
	add_zzz_prefix_to_list_items(&ws, pool).await?; 

    info!(""); 

    // Update the remaining names that begin with a lower case letter

    let sql = r#"update ad.locs set fac_proc = upper(substring(fac_proc, 1, 1))||substring(fac_proc, 2) where fac_proc ~ '^[a-z]';  "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had initial lower case letter replaced by upper case version", res); 
	
	// recover 'protected' names

    let sql = r#"update ad.locs set fac_proc = substring(fac_proc, 4) where fac_proc ~ '^ZZZ';  "#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had protective ZZZ prefix removed", res); 

    info!(""); 

    /*

	select * from ad.locs where fac_proc ~ 'e'
	order by fac_proc
	select * from ad.locs where fac_proc ~ 'dep '
	order by fac_proc
	select * from ad.locs where fac_proc ~ '[a-z]'
	order by fac_proc
	
*/
        
    Ok(())
}


pub async fn regularise_word_research(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    let ws = vec!["research", "RESEARCH", "reseach", "Reseach", "Reseacrh", "Reseaerch", "Researh", 
    "Reseatch", "Reserach", "Reseearch", "Reserch", "Resesarch"];
    replace_list_items_with_target("Research", &ws, pool).await?;

    let ws = vec!["Reasearch", "Reaseach", "Reaserach", "Reearch", "Resarch", "Reseaarch", "Researcg", 
    "Researche", "Reserarch", "Resezrch", "Ressearch", "RFesearch", "Rsearch"];
    replace_list_items_with_target("Research", &ws, pool).await?;

    info!(""); 
    Ok(())
}


pub async fn regularise_word_investigation(pool: &Pool<Postgres>) -> Result<(), AppError> {  
    
    // Investigational (Site)

	// Start by ensuring capitalised versions of relevant words (N.B. not the spanish ones).
	// The 'investigational site' entry type is used by pharma companies, and is therefore essentially English
	// Also exclude the long 'For further informatrion' entries
	
    let sql = r#"update ad.locs set fac_proc = replace(fac_proc, 'inv', 'Inv') 
	where (fac_proc like '% inves%' or fac_proc like 'inves%'
	or fac_proc like '%invers%' or fac_proc like '%inveti%' 
	or fac_proc like '%invsti%')
	and fac_proc ~* 'Site'
	and fac_proc !~ 'For '
	and fac_proc !~ 'investigac' and fac_proc !~ 'investigaç'
	and fac_proc !~ 'inform';"#;
    let res = execute_sql(sql, pool).await?.rows_affected();
    info!("{} records had investig- related words capitalised", res); 

	// This small group needs to be added to the list additionally ('Site' missing in original)
    
    replace_in_fac_proc("Trius investigator", "Trius Investigator Site", "fac_proc ~ 'Trius investigator'", true, "", pool).await?; 

    /* 
    // Check here that all singular 'site' are 'Site' - appears to be the case
	
    select * from ad.locs
    where (fac_proc like '% Inves%' or fac_proc like 'Inves%' 
	or fac_proc like '%Invers%' or fac_proc like '%Inveti%' 
	or fac_proc like '%Invsti%' or fac_proc like '%Inesti%')
	and fac_proc !~ 'Investigac' and fac_proc !~ 'Investigaç'
	and fac_proc ~ 'site'
	and fac_proc !~ 'sites'
	and fac_proc !~ '^For '
	and fac_proc !~ 'inform';
    
    */

    let ws = vec!["Investigation Site", "Investigator Site", "Investigative Site", "Investigate Site", 
    "Investiational Site", "Investigtional Site", "Inverstigational Site", "Invetigational Site", 
    "Invesitgational Site", "Invstigative Site", "Investivative Site", "Investigatice Site"];
    replace_list_items_with_target("Investigational Site", &ws, pool).await?;

    let ws = vec!["Investigating Site", "Investgative Site", "Investigationel Site", 
    "Investigtive Site", "Invesgational Site", "Invesigative Site", "Inestigational Site", "Invesigational Site"];
    replace_list_items_with_target("Investigational Site", &ws, pool).await?;

    info!(""); 
    Ok(())
}


pub async fn regularise_word_university(pool: &Pool<Postgres>) -> Result<(), AppError> { 

    let ws = vec!["univerity", "Univerity", "unversity", "Unversity", "univrsity", "Univrsity", "univeersity", "Univeersity"];
    replace_list_items_with_target("University", &ws, pool).await?;

    let ws = vec!["univerrsity", "Univerrsity", "universsity", "Universsity", "univresity", "Univresity", "univeristy", "Univeristy"];
    replace_list_items_with_target("University", &ws, pool).await?;

    replace_in_fac_proc("university", "University", "r", true, "", pool).await?; 
    replace_in_fac_proc("UNIVERSITY", "University", "r", true, "", pool).await?; 
    replace_in_fac_proc("univ. of", "University of", "r", true, "", pool).await?; 
    replace_in_fac_proc("Univ. of", "University of", "r", true, "", pool).await?; 
    replace_in_fac_proc("Univ.", "University", r"fac_proc ~ 'Univ\.' and country ~ 'United States'", true, "", pool).await?; 
  

    // update ad.locs set fac_proc = replace(fac_proc, 'Duke Univ. Med. Ctr.', 'Duke University Medical Center') where fac_proc ~ 'Duke Univ. Med. Ctr.'; 

    info!("");
    Ok(())
}


pub async fn regularise_word_others(pool: &Pool<Postgres>) -> Result<(), AppError> { 


let ws = vec![" THE ", " AND ", " Y ", " OF ", " DE ", " DU ", " DEL ", " DELLA ",
 " E ", " SOBRE ", " D’", " FOR "];
replace_list_items_with_lower_case(&ws, pool).await?;

let ws = vec!["CENTRE", "CENTRO", "INSTITUTO", "INSTITUTE", "ISTITUTO", "INSTITUT",
   "FOUNDATION", "FUNDACION", "TRATAMIENTO", "TREATMENT", "GENERAL", "NATIONAL", "REGIONAL", "FACULTY",
   "MEDICINE", "DENTISTRY", "CLINICAL", "TRIALS", "CLÍNICA", "CLINIC", "CARDIOLOGY", "INVESTIGACIONES", 
   "INVESTIGACION", "INVESTIGATION", "HEMATOLOGIE", "THORACIC"];
replace_list_items_with_capitalised(&ws, pool).await?;

let ws = vec!["CANCEROLOGIE", "BIOCANCER", "UNICANCER", "CANCERCARE", "CANCER", 
   "CURIE", "ONCOLOGY", "ONCOLOGÍA", "ONCOLOGIA", "ALLIANCE", "CARE", "CHILDREN’S"];
replace_list_items_with_capitalised(&ws, pool).await?;

let ws = vec!["SEATTLE", "MONTPELLIER", "AURELLE", " VAL ", "ONTARIO", 
"JIAMUSI", "STRASBOURG", "EUROPE", "UKRAINE", "LISBOA", "URAL", "CATALAN", "CATALA",
"BASSE", "NORMANDIE", "GUSTAVE", "ROUSSY", "PARIS", " NEW ", "ENGLAND", "YORK", "DELHI",
"CALIFORNIA", "WISCONSIN", "PENNSYLVANIA", "TEXAS", "CHINESE", "CHINA", "ATHENS", "MASSACHUESETTS",
"WASHINGTON", "BIRMINGHAM", "ALABAMA", "DUKE"];
replace_list_items_with_capitalised(&ws, pool).await?;

let ws = vec!["FORSCHUNGSINSTITUT", "FORSCHUNG", "TRANSLATIONAL", "METABOLISM", "DIABETES",
"SCIENCES", "DENTAL", "GROUPE", "GROUP", "HOSPITALIER", "MUTUALISTE", "COLLEGE", "SCHOOLS", "SCHOOL"];
replace_list_items_with_capitalised(&ws, pool).await?;


let ws = vec!["recruting", "recuiting"];
replace_list_items_with_target("recruiting", &ws, pool).await?;

let ws = vec!["MEDICAL", "medical", "medicall", "Medicall", "med.", "Med."];
replace_list_items_with_target("Medical", &ws, pool).await?;

let ws = vec!["HOSPITAL", "hospital", "hospitall", "hosptal"];
replace_list_items_with_target("Hospital", &ws, pool).await?;

// hospita at end

replace_in_fac_proc("hospita", "Hospital", "e", true, "", pool).await?; 
replace_in_fac_proc("Hospita", "Hospital", "e", true, "", pool).await?; 

let ws = vec!["CENTER", "ctr.", "Ctr."];
replace_list_items_with_target("Center", &ws, pool).await?;

replace_in_fac_proc("Repbulic", "Republic", "r", true, "", pool).await?; 
replace_in_fac_proc("SUN YAT-SEN", "Sun Yat-sen", "r", true, "", pool).await?; 
replace_in_fac_proc("L’OUEST", "l’Ouest", "r", true, "", pool).await?; 
 

/*
    -- others

     let ws = vec!["", "", "*", "", "", "", "", "", "", "", "", ""];

      let ws = vec!["", "", "*", "", "", "", "", "", "", "", "", ""];

       let ws = vec!["", "", "*", "", "", "", "", "", "", "", "", ""];

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
