<html>

	<head>
		<meta http-equiv="content-type" content="text/html; charset=utf-8">
		<title>WKA to Ausgabe-DB täglich</title>
	</head>

	<body link="darkred" vlink="darkred" alink="red">

<?php
	// Verbindung aufbauen, auswählen einer Datenbank
	$link = mysql_connect("SERVER", "USER", "PASSWORT")
	or die("Keine Verbindung möglich!");
	mysql_select_db("DATENBANKNAME")
	or die("Auswahl der Datenbank fehlgeschlagen");

	//mysql_query ('SET NAMES utf8');

	date_default_timezone_set("Europe/Berlin");

	mysql_query ('TRUNCATE TABLE ee_wind_taeglich');



	$heute = date("Y-m-d");
	$gestern = date("Y-m-d", strtotime("-1 day"));
	$aktuelles_jahr = date("Y");
	$erster_tag_des_jahres = date($aktuelles_jahr."-01-01");
	$enddatum = date("2031-01-01");
	$tage_bis_enddatum_land = (strtotime($enddatum) - strtotime("2023-02-01")) / 86400;
	$tage_bis_enddatum_see = (strtotime($enddatum) - strtotime("2023-01-01")) / 86400;

	$gesamtleistung_gw = 0;

	//////////////
	// ON-SHORE //
	//////////////
	
	echo '<table><tr><th>Lage</th><th>Datum</th><th>Bereits installierte Leistung</th><th>Zuwachs</th><th>Fest geplante zukünftige Inbetriebnahmen</th><th>Zuwachs</th><th>Durchschnittlich nötiger Ausbau für Klimaschutzziel 2030</th><th>Zuwachs</th></tr>';
	
	// VORHANDEN BIS 2009
	$vorleistung_bis_2009 = 0;
	$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft an Land' OR lage_einheit = 'WindAnLand') AND betriebsstatus <> 'In Planung' AND betriebsstatus <> 'InPlanung' AND betriebsstatus <> 'Vorübergehend stillgelegt' AND betriebsstatus <> 'VoruebergehendStillgelegt' AND datum_inbetriebnahme < '2010-01-01'";
	$result = mysql_query($query);
	while ($row=mysql_fetch_array($result)){
		$vorleistung_bis_2009 = $vorleistung_bis_2009 + $row[nettonennleistung];
	}
	$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft an Land' OR lage_einheit = 'WindAnLand') AND datum_stilllegung <> '0000-00-00' AND datum_stilllegung < '2010-01-01'";
	$result = mysql_query($query);
	while ($row=mysql_fetch_array($result)){
		$vorleistung_bis_2009 = $vorleistung_bis_2009 - $row[nettonennleistung];
	}

	$gesamtleistung_gw = $vorleistung_bis_2009 / 1000000;
	$geplante_gesamtleistung_gw_land = 0;
	$schon_virtuell_installiert = 0;

	// ZUBAU AB 2010
	$start = strtotime("2010-01-01");
	$ende = strtotime("2030-12-31");
	for ($i = $start; $i <= $ende; $i += 86400) {
		$nettonennleistung_tag_mw = 0;
		$abzug_tag_mw = 0;
		$geplante_leistung_tag_mw = 0;
		$datum = date("Y-m-d", $i);

		$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft an Land' OR lage_einheit = 'WindAnLand') AND betriebsstatus <> 'In Planung' AND betriebsstatus <> 'InPlanung' AND betriebsstatus <> 'Vorübergehend stillgelegt' AND betriebsstatus <> 'VoruebergehendStillgelegt' AND datum_inbetriebnahme = '".$datum."'";
		$result = mysql_query($query);
		while ($row=mysql_fetch_array($result)){
			$nettonennleistung_tag_mw = $nettonennleistung_tag_mw + $row[nettonennleistung] / 1000;
		}

		$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft an Land' OR lage_einheit = 'WindAnLand') AND datum_stilllegung = '".$datum."'";
		$result = mysql_query($query);
		while ($row=mysql_fetch_array($result)){
			$abzug_tag_mw = $abzug_tag_mw + $row[nettonennleistung] / 1000;
		}
		
		$gesamtleistung_gw = round ($gesamtleistung_gw + $nettonennleistung_tag_mw / 1000 - $abzug_tag_mw / 1000, 2);
		$zuwachs = $nettonennleistung_tag_mw - $abzug_tag_mw;
		$zuwachs_string = round ($nettonennleistung_tag_mw - $abzug_tag_mw, 1);
		IF ($i == strtotime("2023-02-01")) {$stand_2023_02_01 = $gesamtleistung_gw;}
			
		IF ($datum >= $heute) {
			// SCHON MIT DATUM GEPLANTE INSTALLATIONEN (NUR IN ZUKUNFT)
			$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft an Land' OR lage_einheit = 'WindAnLand') AND (betriebsstatus = 'In Planung' OR betriebsstatus = 'InPlanung') AND datum_geplante_inbetriebnahme = '".$datum."'";
			$result = mysql_query($query);
			while ($row=mysql_fetch_array($result)){
				$geplante_leistung_tag_mw = $geplante_leistung_tag_mw + $row[nettonennleistung] / 1000;
			}
			$geplante_gesamtleistung_gw_land = $geplante_gesamtleistung_gw_land + $geplante_leistung_tag_mw / 1000;
			$summe_inbetrieb_und_geplant_land = round ($gesamtleistung_gw + $geplante_gesamtleistung_gw_land, 2);
		}	

		IF ($datum >= "2023-02-01") {
			// DURCHSCHNITTLICH NÖTIGE INSTALLATIONEN (WIND-AN-LAND-GESETZ AM 1.2.2023 IN KRAFT GETRETEN), UM 115 GW ZU ERREICHEN
			$noch_zu_installieren = 115 - $stand_2023_02_01;
			$taeglich_noetige_leistung_gw_land = $noch_zu_installieren / $tage_bis_enddatum_land;
			$noetige_gesamtleistung_gw_land = round ($stand_2023_02_01 + $schon_virtuell_installiert + $taeglich_noetige_leistung_gw_land, 2);
			$schon_virtuell_installiert = $schon_virtuell_installiert + $taeglich_noetige_leistung_gw_land; 
		}	


		IF ($datum <= "2023-02-01") {
			echo '<tr><td>Windkraft an Land</td><td>'.$datum.'</td><td>'.$gesamtleistung_gw.'</td><td>'.$zuwachs.'</td><td></td><td></td><td></td><td></td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, installiert_gesamt, installiert_taeglich, stand, installiert_taeglich_wert) VALUES ('$datum', 'Windkraft an Land', '$gesamtleistung_gw', '$zuwachs_string', '$heute', '$zuwachs')");
		}
		ELSEIF ($datum <= $heute) {
			$noetig_taeglich = $taeglich_noetige_leistung_gw_land * 1000;
			$noetig_taeglich_string = round ($noetig_taeglich, 1);
			echo '<tr><td>Windkraft an Land</td><td>'.$datum.'</td><td>'.$gesamtleistung_gw.'</td><td>'.$zuwachs.'</td><td></td><td></td><td>'.$noetige_gesamtleistung_gw_land.'</td><td>'.$noetig_taeglich.'</td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, installiert_gesamt, installiert_taeglich, noetig_gesamt, noetig_taeglich, stand, installiert_taeglich_wert, noetig_taeglich_wert) VALUES ('$datum', 'Windkraft an Land', '$gesamtleistung_gw', '$zuwachs_string', '$noetige_gesamtleistung_gw_land', '$noetig_taeglich_string', '$heute', '$zuwachs', '$noetig_taeglich')");
		}
		ELSE {
			$geplant_taeglich = $geplante_leistung_tag_mw;
			$noetig_taeglich = $taeglich_noetige_leistung_gw_land * 1000;
			$geplant_taeglich_string = round ($geplant_taeglich, 1);
			$noetig_taeglich_string = round ($noetig_taeglich, 1);
			echo '<tr><td>Windkraft an Land</td><td>'.$datum.'</td><td></td><td></td><td>'.$summe_inbetrieb_und_geplant_land.'</td><td>'.$geplant_taeglich.'</td><td>'.$noetige_gesamtleistung_gw_land.'</td><td>'.$noetig_taeglich.'</td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, geplant_gesamt, geplant_taeglich, noetig_gesamt, noetig_taeglich, stand, geplant_taeglich_wert, noetig_taeglich_wert) VALUES ('$datum', 'Windkraft an Land', '$summe_inbetrieb_und_geplant_land', '$geplant_taeglich_string', '$noetige_gesamtleistung_gw_land', '$noetig_taeglich_string', '$heute', '$geplant_taeglich', '$noetig_taeglich')");
		}

	}


	///////////////
	// OFF-SHORE //
	///////////////
	
	// VORHANDEN BIS 2009
	$vorleistung_bis_2009 = 0;
	$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft auf See' OR lage_einheit = 'WindAufSee') AND betriebsstatus <> 'In Planung' AND betriebsstatus <> 'InPlanung' AND betriebsstatus <> 'Vorübergehend stillgelegt' AND betriebsstatus <> 'VoruebergehendStillgelegt' AND datum_inbetriebnahme < '2010-01-01'";
	$result = mysql_query($query);
	while ($row=mysql_fetch_array($result)){
		$vorleistung_bis_2009 = $vorleistung_bis_2009 + $row[nettonennleistung];
	}
	$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft auf See' OR lage_einheit = 'WindAufSee') AND datum_stilllegung <> '0000-00-00' AND datum_stilllegung < '2010-01-01'";
	$result = mysql_query($query);
	while ($row=mysql_fetch_array($result)){
		$vorleistung_bis_2009 = $vorleistung_bis_2009 - $row[nettonennleistung];
	}

	$gesamtleistung_gw = $vorleistung_bis_2009 / 1000000;
	$geplante_gesamtleistung_gw_see = 0;
	$schon_virtuell_installiert = 0;

	// ZUBAU AB 2010
	$start = strtotime("2010-01-01");
	$ende = strtotime("2030-12-31");
	for ($i = $start; $i <= $ende; $i += 86400) {
		$nettonennleistung_tag_mw = 0;
		$abzug_tag_mw = 0;
		$geplante_leistung_tag_mw = 0;
		$datum = date("Y-m-d", $i);

		$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft auf See' OR lage_einheit = 'WindAufSee') AND betriebsstatus <> 'In Planung' AND betriebsstatus <> 'InPlanung' AND betriebsstatus <> 'Vorübergehend stillgelegt' AND betriebsstatus <> 'VoruebergehendStillgelegt' AND datum_inbetriebnahme = '".$datum."'";
		$result = mysql_query($query);
		while ($row=mysql_fetch_array($result)){
			$nettonennleistung_tag_mw = $nettonennleistung_tag_mw + $row[nettonennleistung] / 1000;
		}

		$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft auf See' OR lage_einheit = 'WindAufSee') AND datum_stilllegung = '".$datum."'";
		$result = mysql_query($query);
		while ($row=mysql_fetch_array($result)){
			$abzug_tag_mw = $abzug_tag_mw + $row[nettonennleistung] / 1000;
		}
		
		$gesamtleistung_gw = round ($gesamtleistung_gw + $nettonennleistung_tag_mw / 1000 - $abzug_tag_mw / 1000, 2);
		$zuwachs = $nettonennleistung_tag_mw - $abzug_tag_mw;
		$zuwachs_string = round ($nettonennleistung_tag_mw - $abzug_tag_mw, 1);
		IF ($i == strtotime("2023-01-01")) {$stand_2023_01_01 = $gesamtleistung_gw;}
			
		IF ($datum >= $heute) {
			// SCHON MIT DATUM GEPLANTE INSTALLATIONEN (NUR IN ZUKUNFT)
			$query = "SELECT nettonennleistung FROM ee_wind WHERE (lage_einheit = 'Windkraft auf See' OR lage_einheit = 'WindAufSee') AND (betriebsstatus = 'In Planung' OR betriebsstatus = 'InPlanung') AND datum_geplante_inbetriebnahme = '".$datum."'";
			$result = mysql_query($query);
			while ($row=mysql_fetch_array($result)){
				$geplante_leistung_tag_mw = $geplante_leistung_tag_mw + $row[nettonennleistung] / 1000;
			}
			$geplante_gesamtleistung_gw_see = $geplante_gesamtleistung_gw_see + $geplante_leistung_tag_mw / 1000;
			$summe_inbetrieb_und_geplant_see = round ($gesamtleistung_gw + $geplante_gesamtleistung_gw_see, 2);
		}	

		IF ($datum >= "2023-01-01") {
			// DURCHSCHNITTLICH NÖTIGE INSTALLATIONEN (WIND-AN-SEE-GESETZ AM 1.1.2023 IN KRAFT GETRETEN), UM 30 GW ZU ERREICHEN
			$noch_zu_installieren = 30 - $stand_2023_01_01;
			$taeglich_noetige_leistung_gw_see = $noch_zu_installieren / $tage_bis_enddatum_see;
			$noetige_gesamtleistung_gw_see = round ($stand_2023_01_01 + $schon_virtuell_installiert + $taeglich_noetige_leistung_gw_see, 2);
			$schon_virtuell_installiert = $schon_virtuell_installiert + $taeglich_noetige_leistung_gw_see; 
		}	


		IF ($datum <= "2023-01-01") {
			echo '<tr><td>Windkraft auf See</td><td>'.$datum.'</td><td>'.$gesamtleistung_gw.'</td><td>'.$zuwachs.'</td><td></td><td></td><td></td><td></td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, installiert_gesamt, installiert_taeglich, stand, installiert_taeglich_wert) VALUES ('$datum', 'Windkraft auf See', '$gesamtleistung_gw', '$zuwachs_string', '$heute', '$zuwachs')");
		}
		ELSEIF ($datum <= $heute) {
			$noetig_taeglich = $taeglich_noetige_leistung_gw_see * 1000;
			$noetig_taeglich_string = round ($noetig_taeglich, 1);
			echo '<tr><td>Windkraft auf See</td><td>'.$datum.'</td><td>'.$gesamtleistung_gw.'</td><td>'.$zuwachs.'</td><td></td><td></td><td>'.$noetige_gesamtleistung_gw_see.'</td><td>'.$noetig_taeglich.'</td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, installiert_gesamt, installiert_taeglich, noetig_gesamt, noetig_taeglich, stand, installiert_taeglich_wert, noetig_taeglich_wert) VALUES ('$datum', 'Windkraft auf See', '$gesamtleistung_gw', '$zuwachs_string', '$noetige_gesamtleistung_gw_see', '$noetig_taeglich_string', '$heute', '$zuwachs', '$noetig_taeglich')");
		}
		ELSE {
			$geplant_taeglich = $geplante_leistung_tag_mw;
			$noetig_taeglich = $taeglich_noetige_leistung_gw_see * 1000;
			$geplant_taeglich_string = round ($geplant_taeglich, 1);
			$noetig_taeglich_string = round ($noetig_taeglich, 1);
			echo '<tr><td>Windkraft auf See</td><td>'.$datum.'</td><td></td><td></td><td>'.$summe_inbetrieb_und_geplant_see.'</td><td>'.$geplant_taeglich.'</td><td>'.$noetige_gesamtleistung_gw_see.'</td><td>'.$noetig_taeglich.'</td></tr>';
			mysql_query ("INSERT INTO ee_wind_taeglich (datum, lage_einheit, geplant_gesamt, geplant_taeglich, noetig_gesamt, noetig_taeglich, stand, geplant_taeglich_wert, noetig_taeglich_wert) VALUES ('$datum', 'Windkraft auf See', '$summe_inbetrieb_und_geplant_see', '$geplant_taeglich_string', '$noetige_gesamtleistung_gw_see', '$noetig_taeglich_string', '$heute', '$geplant_taeglich', '$noetig_taeglich')");
		}

	}

	echo '</table>';



?>

	</body>
</html>
