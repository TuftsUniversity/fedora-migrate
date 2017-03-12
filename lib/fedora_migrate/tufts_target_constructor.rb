require 'open-uri'
require 'uri'
require 'curation_concerns'
require 'tufts/vocab/tufts_terms'
require 'rdf/fcrepo3'

module FedoraMigrate
  class TuftsTargetConstructor
    attr_accessor :source, :candidates, :target
    attr_accessor :payload_primary, :payload_secondary, :depositor_utln

    def initialize(source, payload_stream, secondary_payload_stream, depositor)
      @source = source
      @payload_primary = payload_stream
      @payload_secondary = secondary_payload_stream
      @depositor_utln = depositor
    end

    # Override .build to use Fedora's default id minter
    # https://github.com/projecthydra-labs/fedora-migrate/issues/49
    #obj.id FedoraMigrate::Mover.id_component(source)
    def build
      raise FedoraMigrate::Errors::MigrationError, "No qualified targets found in #{source.pid}" if target.nil?

      # create target, and apply depositor metadata
      obj = target.new

      obj.apply_depositor_metadata @depositor_utln
      obj.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC

      user = User.find_by_user_key(@depositor_utln)
      CurationConcerns::Workflow::ActivateObject.call(target: obj, comment: 'activate object', user: user)

      create_and_add_payload(obj, @payload_primary, @depositor_utln)

      #deal with 2 primary datastream objects, storing second object in a new file set
      create_and_add_payload(obj, @payload_secondary, @depositor_utln) unless @payload_secondary.nil?

      #handle a case of bad hand created data on old records
      create_and_add_payload(obj, "ARCHIVAL_SOUND", @depositor_utln) if @payload_primary == "ARCHIVAL_WAV"

      # back up old data
      #create_and_add_fcrepo3_set obj

      process_desc_metadata obj
      process_admin_metadata obj
      process_collection_metadata obj
      process_relsext_metadata obj

      obj.save

      active_workflow = Sipity::Workflow.find(2)
      Sipity::Entity.create!(proxy_for_global_id: obj.to_global_id.to_s,
                             workflow: active_workflow,
                             workflow_state: nil)

      obj
    end

    def create_and_add_payload(obj, payload_stream, depositor_utln)
      # create fileset and apply depository metadata

      return if source.datastreams[payload_stream].content.nil?

      file_set = FileSet.new
      file_set.apply_depositor_metadata depositor_utln

      if payload_stream == "GENERIC-CONTENT"
        @doc = Nokogiri::XML(source.datastreams[payload_stream].content).remove_namespaces!
        location = @doc.xpath('//link').text
        uri = URI.parse(location)
        target_file = File.basename(uri.path)
        file_set.title = [target_file]
      else
        file_set.title = [source.datastreams[payload_stream].label]
      end

      file_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PUBLIC
      #set_voting_record_type file_set
      if payload_stream == "GENERIC-CONTENT"
        datastream_content = get_generic_file_from_source(payload_stream)
      else
        datastream_content = get_file_from_source(payload_stream)
      end


      user = User.find_by_user_key(file_set.depositor)
      actor = CurationConcerns::Actors::FileSetActor.new(file_set, user)
      actor.create_metadata(obj)
      actor.create_content(datastream_content)
      file_set.save

      #clean up temp file
      File.delete(datastream_content) if File.exist?(datastream_content)

    end

    def process_admin_metadata obj
      field = process_metadata_field('date_submitted', 'DC-DETAIL-META', false)
      obj.date_submitted = field unless field == ''

      field = process_metadata_field('date_issued', 'DCA-META', false)
      obj.date_issued = field unless field == ''

      field = process_metadata_field('date_available', 'DCA-META', false)
      obj.date_available = field unless field == ''

      field = process_metadata_field('date_modified', 'DC-DETAIL-META', false)
      obj.date_modified = field unless field == ''

      field = process_metadata_field('license', 'DC-DETAIL-META', false)
      obj.license = field unless field == ''

      field = process_metadata_field('accrualPolicy', 'DC-DETAIL-META', false)
      obj.accrual_policy = field unless field == ''

      field = process_metadata_field('steward', 'DCA-ADMIN', false)
      obj.steward = field unless field == ''

      field = process_metadata_field('comment', 'DCA-ADMIN', false)
      obj.internal_note =field unless field == ''

      field = process_metadata_field('displays', 'DCA-ADMIN')
      obj.displays_in = field unless field.empty?

      #embargo
      vis = process_metadata_field('visibility', 'DCA-ADMIN', false)
      obj.visibility = vis unless vis == ''

      field = process_metadata_field('batchID', 'DCA-ADMIN', false)
      obj.batch_id = field unless field == ''

      field = process_metadata_field('audience', 'DC-DETAIL-META', false)
      obj.audience = field unless field == ''

      field = process_metadata_field('dateAccepted', 'DC-DETAIL-META', false)
      obj.date_accepted = field unless field == ''

      field = process_metadata_field('dateCopyrighted', 'DC-DETAIL-META', false)
      obj.date_copyrighted = field unless field == ''

    end

    def process_collection_metadata obj
      # add to collection
    end

    def process_relsext_metadata obj

      xml = Nokogiri::XML(source.datastreams["RELS-EXT"].content).remove_namespaces!

      RDF::FCREPO3::RELSEXT.properties.each do |property|

        val = property.value
        short_code = val[val.index('#')+1..val.length]
        field_values = xml.xpath("//#{short_code}")
        target_values = Array.new

        field_values.each do |field|
          (target_values << field.values.first) unless field.nil?
        end

        obj.send("relsext_#{short_code}=", target_values) unless target_values.length == 0

      end
    end

    def process_desc_metadata obj
      val = source.pid
      obj.legacy_pid = val unless val.nil?

      val = process_metadata_field('title', 'DCA-META')
      obj.title = val unless val.empty?

      val = process_metadata_field('alternative', 'DC-DETAIL-META')
      obj.alternative  = val unless val.empty?

      val = process_metadata_field('creator', 'DCA-META')
      obj.creator = val unless val.empty?

      val = process_metadata_field('contributor', 'DCA-META')
      obj.contributor = val unless val.empty?

      val = process_metadata_field('description', 'DCA-META')
      obj.description = val unless val.empty?

      val = process_metadata_field('abstract', 'DC-DETAIL-META', false)
      obj.abstract = val unless val.empty?

      val = process_metadata_field('publisher', 'DCA-META')
      obj.publisher = val unless val.empty?

      val = process_metadata_field('source', 'DCA-META')
      obj.source = val unless val.empty?

      val = process_metadata_field('date', 'DC-DETAIL-META')
      obj.date = val unless val.empty?

      val = process_metadata_field('language', 'DCA-META')
      obj.language = val unless val.empty?

      val = process_metadata_field('persname', 'DCA-META')
      obj.personal_name = val unless val.empty?

      val = process_metadata_field('corpname', 'DCA-META')
      obj.corporate_name = val unless val.empty?

      val = process_metadata_field('corpname', 'DCA-META')
      obj.corporate_name = val unless val.empty?

      val = process_metadata_field('geogname', 'DCA-META')
      obj.complex_subject = val unless val.empty?

      val = process_metadata_field('subject', 'DCA-META')
      obj.subject = val unless val.empty?

      val = process_metadata_field('genre', 'DCA-DETAIL-META')
      obj.genre = val unless val.empty?

      val = process_metadata_field('spatial', 'DCA-DETAIL-META')
      obj.spatial = val unless val.empty?

      val = process_metadata_field('bibliographic_citation', 'DCA-META')
      obj.bibliographic_citation = val unless val.empty?

      val = process_metadata_field('temporal', 'DCA-META')
      obj.temporal = val unless val.empty?

      val = process_metadata_field('identifier', 'DCA-META', false)
      obj.purl = val unless val.empty?

      val = process_metadata_field('references', 'DCA-DETAIL-META')
      obj.references = val unless val.empty?

      val = process_metadata_field('replaces', 'DCA-DETAIL-META')
      obj.replaces = val unless val.empty?

      val = process_metadata_field('toc', 'DCA-DETAIL-META')
      obj.table_of_contents = val unless val.empty?

      val = process_metadata_field('isReplacedBy', 'DCA-DETAIL-META')
      obj.is_replaced_by = val unless val.empty?

      val = process_metadata_field('extent', 'DCA-META')
      obj.extent = val unless val.empty?

      val = process_metadata_field('provenance', 'DCA-DETAIL-META')
      obj.provenance = val unless val.empty?

      val = process_metadata_field('rightsHolder', 'DCA-DETAIL-META')
      obj.rights_holder = val unless val.empty?

      val = process_metadata_field('funder', 'DCA-DETAIL-META')
      obj.funder = val unless val.empty?

      val = process_metadata_field('rights', 'DCA-META')
      obj.edm_rights = val unless val.empty?

      val  = process_metadata_field('type', 'DCA-META')
      obj.dc_type = val unless val.empty?
    end

    def process_metadata_field(field_name,datastream_name,multiple=true)
      xml = Nokogiri::XML(source.datastreams[datastream_name].content).remove_namespaces!
      field_values = xml.xpath("//#{field_name}")
      if multiple
        target_values = Array.new
        field_values.each do |field|
          (target_values << field.text) unless field.nil?
        end
      else
        unless field_values.empty?
          target_values = field_values.first.text
        else
          target_values = ''
        end
      end

      target_values
    end

    def target
      @target ||= determine_target
    end

    private

    def get_file_from_source(datastream)
      uri = URI.parse(source.datastreams[datastream].location)
      target_file = File.basename(uri.path)

      voting_record = File.new target_file, 'wb'
      voting_record.write source.datastreams[datastream].content.body
      #byebug
      File.new target_file

    end

    def get_generic_file_from_source(datastream)
      @doc = Nokogiri::XML(source.datastreams[datastream].content).remove_namespaces!
      location = @doc.xpath('//link').text

      uri = URI.parse(location)
      target_file = File.basename(uri.path)

      voting_record = File.new target_file, 'wb'
      voting_record.write Net::HTTP.get(uri)

      File.new target_file
    end

    def create_and_add_fcrepo3_set obj
      %w{DC RELS-EXT DCA-ADMIN DCA-META DC-DETAIL-META}.each_with_index do |ds, i|
        next if source.datastreams[ds].content.nil?
        fcrepo3_set = FileSet.new
        fcrepo3_set.apply_depositor_metadata @depositor_utln
        fcrepo3_set.title = ['Fedora 3 Datastreams']
        fcrepo3_set.visibility = Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE

        # Have DC go first so there's always an 'original' ds

        user = User.find_by_user_key(fcrepo3_set.depositor)
        ds_actor = CurationConcerns::Actors::FileSetActor.new(fcrepo3_set, user)
        File.open(ds+'.xml', 'w') { |f| f.write(source.datastreams[ds].content) }
        ds_file = File.open(ds+'.xml', 'r:UTF-8')
        ds_actor.create_metadata(obj, {"visibility" => Hydra::AccessControls::AccessRight::VISIBILITY_TEXT_VALUE_PRIVATE})
        ds_actor.create_content(File.open(ds_file))
        fcrepo3_set.save
      end


    end

    def determine_target

      Array(candidates).map { |model| vet(model) }.compact.first
    end

    def vet(model)
      FedoraMigrate::Mover.id_component(model).constantize
    rescue NameError
      Logger.debug "rejecting #{model} for target"
      nil
    end

    def candidates
      @candidates = Array(source.models).map do |candidate|
        if candidate == "info:fedora/cm:Text.PDF"
          "TuftsPdf"
        elsif candidate == "info:fedora/cm:Text.TEI"
          "TuftsTei"
        elsif candidate == "info:fedora/cm:VotingRecord"
          "TuftsVotingRecord"
        elsif candidate == "info:fedora/cm:Text.EAD"
          "TuftsEad"
        elsif candidate == "info:fedora/cm:Image.4DS"
          "TuftsImage"
        elsif candidate == "info:fedora/cm:Audio"
          "TuftsAudio"
        elsif candidate == "info:fedora/cm:Audio.OralHistory"
          "TuftsAudio"
        elsif candidate == "info:fedora/afmodel:TuftsVideo"
          "TuftsVideo"
        elsif candidate == "info:fedora/cm:Text.RCR"
          "TuftsRcr"
        elsif candidate == "info:fedora/cm:Object.Generic"
          "TuftsGenericObject"
        else
          candidate
        end
      end
    end
  end


end

