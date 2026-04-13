<html>

	<head>
		<meta http-equiv="content-type" content="text/html; charset=utf-8">
		<title>WKA tägliche Aktualisierung</title>
	</head>

	<body link="darkred" vlink="darkred" alink="red">

<?php
	ob_start();

	// Verbindung aufbauen, auswählen einer Datenbank
	$link = mysql_connect("SERVER", "USER", "PASSWORT")
	or die("Keine Verbindung möglich!");
	mysql_select_db("DATENBANKNAME")
	or die("Auswahl der Datenbank fehlgeschlagen");

	//mysql_query ('SET NAMES utf8');

	date_default_timezone_set("Europe/Berlin");

	$heute = date("Y-m-d");
	$gestern = date("Y-m-d", strtotime("-2 day"));

	try {
		$client = new SoapClient("https://www.marktstammdatenregister.de/MaStRAPI/wsdl/mastr.wsdl");

		$apikey = "HIER_MUSS_DER_LANGE_PERSÖNLICHER_API_KEY_REIN";

		$marktakteurMastrNummer = "HIER_DIE_EIGENE_AKTEUR-NUMMER_IM_FORMAT SOMXXXXXXXXXXXX";
		
		$limit = 800;

		// ABFRAGE NEUE Einheiten seit gestern/vorgestern gefiltert
		$result = $client->GetGefilterteListeStromErzeuger(array("apiKey" => $apikey, "marktakteurMastrNummer" => $marktakteurMastrNummer, "energietraeger" => "Wind", "datumAb" => $gestern, "limit" => $limit));


		echo '<table>';
		echo '<tr><th>Nr.</th><th>MaStR-Nr.</th><th>Name</th><th>Lage</th><th>Standort</th><th>Bundesland</th><th>Bruttoleistung</th><th>Status</th><th>Eintrag vorhanden?</th><th>Inbetriebnahmedatum</th></tr>';
		$i = 0;
		foreach ($result->Einheiten as $einheit) {
			
			$EinheitMastrNummer = $einheit->EinheitMastrNummer;
			
			$i++;
			$query = "SELECT * FROM ee_wind WHERE mastrnr_einheit = '".$EinheitMastrNummer."' LIMIT 1";
			$result_sql = mysql_query($query);
			if (mysql_affected_rows() > 0) {$vorhanden = "ja";}
			else {$vorhanden = "nein";}


				
			IF ($vorhanden == "nein") {
				$pause = mt_rand(2, 5);
	//			sleep($pause);

				$result2 = $client->GetEinheitWind(array("apiKey" => $apikey, "marktakteurMastrNummer" => $marktakteurMastrNummer, "einheitMastrNummer" => $EinheitMastrNummer));


				$Name = $result2->NameStromerzeugungseinheit;
				$EinheitBetriebsstatus = $result2->EinheitBetriebsstatus;
				$Bruttoleistung = $result2->Bruttoleistung;
				$Nettonennleistung = $result2->Nettonennleistung;
				$Inbetriebnahmedatum = $result2->Inbetriebnahmedatum;
				$Registrierungsdatum = $result2->Registrierungsdatum;
				$Bundesland = $result2->Bundesland;
				$Landkreis = $result2->Landkreis;
				$Gemeinde = $result2->Gemeinde;
				$Postleitzahl = $result2->Postleitzahl;
				$Ort = $result2->Ort;
				$Strasse = $result2->Strasse;
				$Gemarkung = $result2->Gemarkung;
				$FlurFlurstuecknummern = $result2->FlurFlurstuecknummern;
				$Gemeindeschluessel = $result2->Gemeindeschluessel;
				$Breitengrad = $result2->Breitengrad;
				$Laengengrad = $result2->Laengengrad;
				$NameWindpark = $result2->NameWindpark;
				$Nabenhoehe = $result2->Nabenhoehe;
				$Rotordurchmesser = $result2->Rotordurchmesser;
				$Hersteller = $result2->Hersteller->Wert;
				$Typenbezeichnung = $result2->Typenbezeichnung;
				$Technologie = $result2->Technologie;
				$WindAnLandOderSee = $result2->WindAnLandOderSee;
				$DatumLetzteAktualisierung = $result2->DatumLetzteAktualisierung;
				if ($result2->DatumBeginnVoruebergehendeStilllegung > $result2->DatumEndgueltigeStilllegung) {
					$Datum_Stilllegung = $result2->DatumBeginnVoruebergehendeStilllegung;
				}
				else {
					$Datum_Stilllegung = $result2->DatumEndgueltigeStilllegung;
				}
				$GeplantesInbetriebnahmedatum = $result2->GeplantesInbetriebnahmedatum;


				mysql_query ("INSERT INTO ee_wind (mastrnr_einheit, name_einheit, betriebsstatus, bruttoleistung, nettonennleistung, datum_inbetriebnahme, datum_registrierung, bundesland, landkreis, gemeinde, plz, ort, strasse, gemarkung, flurstueck, gemeindeschluessel, breitengrad, laengengrad, name_windpark, nabenhoehe, rotordurchmesser, hersteller_windanlage, typenbezeichnung, technologie, lage_einheit, letzte_aktualisierung, datum_stilllegung, datum_geplante_inbetriebnahme) VALUES ('$EinheitMastrNummer', '$Name', '$EinheitBetriebsstatus', '$Bruttoleistung', '$Nettonennleistung', '$Inbetriebnahmedatum', '$Registrierungsdatum', '$Bundesland', '$Landkreis', '$Gemeinde', '$Postleitzahl', '$Ort', '$Strasse', '$Gemarkung', '$FlurFlurstuecknummern', '$Gemeindeschluessel', '$Breitengrad', '$Laengengrad', '$NameWindpark', '$Nabenhoehe', '$Rotordurchmesser', '$Hersteller', '$Typenbezeichnung', '$Technologie', '$WindAnLandOderSee', '$DatumLetzteAktualisierung', '$Datum_Stilllegung', '$GeplantesInbetriebnahmedatum')");
			}


			ELSE {
				$pause = mt_rand(2, 5);
	//			sleep($pause);

				$result2 = $client->GetEinheitWind(array("apiKey" => $apikey, "marktakteurMastrNummer" => $marktakteurMastrNummer, "einheitMastrNummer" => $EinheitMastrNummer));

				$Name = $result2->NameStromerzeugungseinheit;
				$EinheitBetriebsstatus = $result2->EinheitBetriebsstatus;
				$Bruttoleistung = $result2->Bruttoleistung;
				$Nettonennleistung = $result2->Nettonennleistung;
				$Inbetriebnahmedatum = $result2->Inbetriebnahmedatum;
				$Registrierungsdatum = $result2->Registrierungsdatum;
				$Bundesland = $result2->Bundesland;
				$Landkreis = $result2->Landkreis;
				$Gemeinde = $result2->Gemeinde;
				$Postleitzahl = $result2->Postleitzahl;
				$Ort = $result2->Ort;
				$Strasse = $result2->Strasse;
				$Gemarkung = $result2->Gemarkung;
				$FlurFlurstuecknummern = $result2->FlurFlurstuecknummern;
				$Gemeindeschluessel = $result2->Gemeindeschluessel;
				$Breitengrad = $result2->Breitengrad;
				$Laengengrad = $result2->Laengengrad;
				$NameWindpark = $result2->NameWindpark;
				$Nabenhoehe = $result2->Nabenhoehe;
				$Rotordurchmesser = $result2->Rotordurchmesser;
				$Hersteller = $result2->Hersteller->Wert;
				$Typenbezeichnung = $result2->Typenbezeichnung;
				$Technologie = $result2->Technologie;
				$WindAnLandOderSee = $result2->WindAnLandOderSee;
				$DatumLetzteAktualisierung = $result2->DatumLetzteAktualisierung;
				if ($result2->DatumBeginnVoruebergehendeStilllegung > $result2->DatumEndgueltigeStilllegung) {
					$Datum_Stilllegung = $result2->DatumBeginnVoruebergehendeStilllegung;
				}
				else {
					$Datum_Stilllegung = $result2->DatumEndgueltigeStilllegung;
				}
				$GeplantesInbetriebnahmedatum = $result2->GeplantesInbetriebnahmedatum;


				mysql_query ("UPDATE ee_wind SET name_einheit = '$Name', betriebsstatus = '$EinheitBetriebsstatus', bruttoleistung = '$Bruttoleistung', nettonennleistung = '$Nettonennleistung', datum_inbetriebnahme = '$Inbetriebnahmedatum', datum_registrierung = '$Registrierungsdatum', bundesland = '$Bundesland', landkreis = '$Landkreis', gemeinde = '$Gemeinde', plz = '$Postleitzahl', ort = '$Ort', strasse = '$Strasse', gemarkung = '$Gemarkung', flurstueck = '$FlurFlurstuecknummern', gemeindeschluessel = '$Gemeindeschluessel', breitengrad = '$Breitengrad', laengengrad = '$Laengengrad', name_windpark = '$NameWindpark', nabenhoehe = '$Nabenhoehe', rotordurchmesser = '$Rotordurchmesser', hersteller_windanlage = '$Hersteller', typenbezeichnung = '$Typenbezeichnung', technologie = '$Technologie', lage_einheit = '$WindAnLandOderSee', letzte_aktualisierung = '$DatumLetzteAktualisierung', datum_stilllegung = '$Datum_Stilllegung', datum_geplante_inbetriebnahme = '$GeplantesInbetriebnahmedatum' WHERE mastrnr_einheit = '$EinheitMastrNummer';"); 
			}


			echo '<tr><td>'.$i.'</td><td>'.$EinheitMastrNummer.'</td><td>'.$Name.'</td><td>'.$WindAnLandOderSee.'</td><td>'.$Postleitzahl." ".$Ort.'</td><td>'.$Bundesland.'</td><td>'.$Bruttoleistung.'</td><td>'.$EinheitBetriebsstatus.'</td><td>'.$vorhanden.'</td><td>'.$Inbetriebnahmedatum.'</td></tr>';

			ob_flush();
			flush();
			
		}
		echo '</table>';
		print "</br>";
		print "</br>";

		print "Mitteilungsfilter";

	} catch (SoapFault $fault) {
		echo "SOAP-Fehler: " . $fault->faultcode . ": " . $fault->faultstring;
	}


	ob_end_flush();
?>

	</body>
</html>